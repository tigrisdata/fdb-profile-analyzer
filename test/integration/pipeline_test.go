//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
	"github.com/pboros/fdb-profile-analyzer/internal/prefix"
)

// TestHotspotRankingSurvivesThePipeline is the headline case: a workload with a
// deliberate, known skew must come out of ClickHouse with that same ranking.
// It covers load generation, sampling, tailing, decoding, prefix matching and
// the writer in one assertion.
func TestHotspotRankingSurvivesThePipeline(t *testing.T) {
	rep := generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.HotOps, c.WarmOps, c.ColdKeys = 12, 5, 8
	})
	if rep.Hotspot == nil {
		t.Fatal("hotspot mode produced no report")
	}
	conn := clickhouse_(t)
	h := rep.Hotspot

	// Every generated operation must eventually arrive; poll until it has.
	ingestUntil(t, []string{testPfx}, 90*time.Second, func() error {
		got := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", h.HotKey)
		if int(got) != h.HotOps {
			return fmt.Errorf("hot key reads = %d, want %d", got, h.HotOps)
		}
		return nil
	})

	gotWarm := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", h.WarmKey)
	if int(gotWarm) != h.WarmOps {
		t.Errorf("warm key reads = %d, want %d", gotWarm, h.WarmOps)
	}

	// The ordering is what the dashboard ranks on, so assert it directly.
	top := scalar[string](t, conn,
		"SELECT key FROM "+chDB+".operations WHERE op_type = 'get' AND key_prefix = ? "+
			"GROUP BY key ORDER BY count() DESC, key LIMIT 1", testPfx)
	if top != h.HotKey {
		t.Errorf("hottest read key = %q, want %q", top, h.HotKey)
	}

	// The same skew must appear in the mutations table, which is what the
	// hot-write-key panel uses.
	gotWrites := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".mutations WHERE mutation_type = 'SET_VALUE' AND key = ?", h.HotKey)
	if int(gotWrites) != h.HotOps {
		t.Errorf("hot key writes = %d, want %d", gotWrites, h.HotOps)
	}

	// Cold keys must stay cold, or the "skew" is an artefact.
	coldMax := scalar[uint64](t, conn,
		"SELECT max(c) FROM (SELECT count() AS c FROM "+chDB+".operations "+
			"WHERE op_type = 'get' AND key LIKE ? GROUP BY key)", testPfx+"/cold/%")
	if coldMax != 1 {
		t.Errorf("busiest cold key has %d reads, want 1", coldMax)
	}
}

// TestMultiChunkRecordIsReassembled covers the hardest decoding path: a record
// too large for one 100KB value, whose chunks the client may write under
// different versionstamps with unrelated records interleaved between them.
func TestMultiChunkRecordIsReassembled(t *testing.T) {
	const mutations = 1500
	generate(t, loadgen.ModeBig, func(c *loadgen.Config) { c.BigSets = mutations })
	conn := clickhouse_(t)

	// The commit event must survive with its mutation count intact.
	ingestUntil(t, []string{testPfx}, 90*time.Second, func() error {
		got := scalar[uint32](t, conn,
			"SELECT max(num_mutations) FROM "+chDB+".operations WHERE op_type = 'commit' AND key_prefix = ?",
			testPfx)
		if int(got) != mutations {
			return fmt.Errorf("largest commit has %d mutations, want %d", got, mutations)
		}
		return nil
	})

	// A record that big necessarily spans several chunks, so if every mutation
	// row is present the concatenation was correct.
	txnID := scalar[string](t, conn,
		"SELECT transaction_id FROM "+chDB+".operations WHERE op_type = 'commit' AND key_prefix = ? "+
			"ORDER BY num_mutations DESC LIMIT 1", testPfx)
	rows := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".mutations WHERE transaction_id = ?", txnID)
	if int(rows) != mutations {
		t.Errorf("mutation rows for %s = %d, want %d", txnID, rows, mutations)
	}

	bytes := scalar[uint32](t, conn,
		"SELECT max(commit_bytes) FROM "+chDB+".operations WHERE transaction_id = ?", txnID)
	if bytes < 100_000 {
		t.Errorf("commit_bytes = %d, expected over the 100KB chunking threshold", bytes)
	}
}

// TestConflictProducesErrorCommit checks the error path decodes, since error
// events have a different layout from their successful counterparts.
func TestConflictProducesErrorCommit(t *testing.T) {
	rep := generate(t, loadgen.ModeConflict, nil)
	if !rep.Conflict {
		t.Skip("the losing transaction committed; no conflict to observe")
	}
	conn := clickhouse_(t)
	// 1020 is not_committed: the transaction lost a write conflict.
	ingestUntil(t, []string{testPfx}, 90*time.Second, func() error {
		got := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE op_type = 'error_commit' "+
				"AND error_code = 1020 AND key_prefix = ?", testPfx)
		if got == 0 {
			return fmt.Errorf("no error_commit with code 1020 yet")
		}
		return nil
	})
}

// TestCursorResumesWithoutReplaying is the property that makes the daemon
// restartable: a second pass from the stored cursor must find only new records.
func TestCursorResumesWithoutReplaying(t *testing.T) {
	generate(t, loadgen.ModeBasic, func(c *loadgen.Config) { c.Txns, c.SetsPerTxn = 3, 2 })

	cursor, _, first := ingest(t, nil, []string{testPfx})
	if first == 0 {
		t.Fatal("first pass ingested nothing")
	}
	if len(cursor) == 0 {
		t.Fatal("no cursor after the first pass")
	}

	// Nothing new has been generated, so resuming must be a no-op.
	_, _, second := ingest(t, cursor, []string{testPfx})
	if second != 0 {
		t.Errorf("resuming from the cursor replayed %d records, want 0", second)
	}

	// New traffic after the cursor must still be picked up.
	generate(t, loadgen.ModeBasic, func(c *loadgen.Config) { c.Txns, c.SetsPerTxn = 2, 2 })
	_, _, third := ingest(t, cursor, []string{testPfx})
	if third == 0 {
		t.Error("resuming missed records written after the cursor")
	}
}

// TestPrefixClassification checks the dimension the whole dashboard groups by.
func TestPrefixClassification(t *testing.T) {
	// Deliberately NOT a string extension of testPfx. Matching is plain
	// string-prefix, so "/foo-bar" would legitimately match a configured
	// "/foo"; a separate top-level prefix is what actually tests the
	// catch-all.
	other := "/unconfigured" + runID

	generate(t, loadgen.ModeBasic, func(c *loadgen.Config) {
		c.Prefixes = []string{testPfx, other}
		c.Txns, c.SetsPerTxn = 2, 2
	})

	conn := clickhouse_(t)
	ingestUntil(t, []string{testPfx}, 90*time.Second, func() error {
		n := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE key_prefix = ? AND key LIKE ?",
			testPfx, testPfx+"/%")
		if n == 0 {
			return fmt.Errorf("no operations classified under %q yet", testPfx)
		}
		return nil
	})

	leaked := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".operations WHERE key_prefix = ? AND key LIKE ?",
		testPfx, other+"/%")
	if leaked != 0 {
		t.Errorf("%d operations under %q were misattributed to %q", leaked, other, testPfx)
	}

	// Unconfigured traffic is labelled by its own first 16 bytes, pointing at
	// where the unclassified workload comes from. `other` is longer than 16
	// bytes (runID is UnixNano), so every key under it shares one label.
	unknown := prefix.Unknown(other)
	inUnknown := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".operations WHERE key_prefix = ? AND key LIKE ?",
		unknown, other+"/%")
	if inUnknown == 0 {
		t.Errorf("traffic under the unconfigured prefix %q was not labelled %q", other, unknown)
	}

	// And none of it may fall into the informationless catch-all, which is
	// reserved for events with no key at all.
	inOther := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".operations WHERE key_prefix = '(other)' AND key LIKE ?",
		other+"/%")
	if inOther != 0 {
		t.Errorf("%d operations under %q still landed in (other)", inOther, other)
	}
}

// TestSystemKeyspaceIsExcluded guards the self-profiling filter. FoundationDB
// profiles its own profile-maintenance transactions; left in, they top every
// hotspot ranking on an otherwise idle cluster.
func TestSystemKeyspaceIsExcluded(t *testing.T) {
	generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.HotOps, c.WarmOps, c.ColdKeys = 4, 2, 2
	})
	conn := clickhouse_(t)
	_, counts, _ := ingest(t, nil, []string{testPfx})
	leaked := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".operations WHERE key LIKE ?", "\xff%")
	if leaked != 0 {
		t.Errorf("%d system-keyspace operations were stored; the filter is not working", leaked)
	}
	t.Logf("skipped %d system records during this ingest", counts.SkippedSystem)
}

// TestSingleKeyClearIsReconstructed pins the wire-format subtlety end to end:
// FoundationDB serializes tr.Clear(key) as (CLEAR_RANGE, keyAfter(key), ""),
// and the decoder must reconstruct the original key or every single-key clear
// lands in ClickHouse under key+'\x00', never grouping with its reads/writes.
// FoundationDB's own Python analyzer gets this wrong.
func TestSingleKeyClearIsReconstructed(t *testing.T) {
	generate(t, loadgen.ModeBasic, func(c *loadgen.Config) { c.Txns, c.SetsPerTxn = 1, 1 })

	// The basic workload clears exactly this key with tr.Clear.
	cleared := testPfx + "/k000003"

	conn := clickhouse_(t)
	ingestUntil(t, []string{testPfx}, 90*time.Second, func() error {
		n := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".mutations WHERE mutation_type = 'CLEAR_RANGE' AND key = ?",
			cleared)
		if n == 0 {
			return fmt.Errorf("no CLEAR_RANGE row for %q yet", cleared)
		}
		return nil
	})

	// The un-reconstructed form must not exist.
	wrong := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".mutations WHERE mutation_type = 'CLEAR_RANGE' AND key = ?",
		cleared+"\x00")
	if wrong != 0 {
		t.Errorf("%d CLEAR_RANGE rows landed under keyAfter(key) — the optimization was not reversed", wrong)
	}
}

// TestHotRangeIsIdentified covers the range-level view: bucketing keys by a
// byte-prefix depth must surface the hot REGION, not just the hot key. Mirrors
// the dashboard's "Hot key ranges" query shape.
func TestHotRangeIsIdentified(t *testing.T) {
	// Its own prefix: the suite's other workloads share testPfx, and the
	// multi-chunk test's 1500-mutation commit would legitimately be the
	// hottest range in that shared keyspace.
	rangePfx := testPfx + "r"

	rep := generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.Prefixes = []string{rangePfx}
		c.HotOps, c.WarmOps, c.ColdKeys = 30, 6, 10
	})
	if rep.Hotspot == nil {
		t.Fatal("hotspot mode produced no report")
	}

	// Depth cuts the keyspace into .../hot/, .../warm, .../cold buckets.
	depth := len(rangePfx) + len("/hot/")
	wantBucket := rep.Hotspot.HotKey[:depth]

	conn := clickhouse_(t)
	ingestUntil(t, []string{rangePfx}, 90*time.Second, func() error {
		top := scalar[string](t, conn, `
			SELECT substring(key, 1, ?) AS r FROM (
				SELECT key, event_time FROM `+chDB+`.operations
				WHERE op_type IN ('get', 'get_range') AND key != '' AND key_prefix = ?
				UNION ALL
				SELECT key, event_time FROM `+chDB+`.mutations
				WHERE mutation_type IN ('SET_VALUE', 'ADD_VALUE') AND key_prefix = ?
			)
			GROUP BY r ORDER BY count() DESC, r LIMIT 1`,
			depth, rangePfx, rangePfx)
		if top != wantBucket {
			return fmt.Errorf("hottest range bucket = %q, want %q", top, wantBucket)
		}
		return nil
	})

	// The hot bucket's traffic must be its reads plus its writes: 30 + 30.
	ops := scalar[uint64](t, conn, `
		SELECT count() FROM (
			SELECT key FROM `+chDB+`.operations
			WHERE op_type IN ('get', 'get_range') AND key_prefix = ?
			UNION ALL
			SELECT key FROM `+chDB+`.mutations
			WHERE mutation_type IN ('SET_VALUE', 'ADD_VALUE') AND key_prefix = ?
		) WHERE startsWith(key, ?)`,
		rangePfx, rangePfx, wantBucket)
	if int(ops) != 2*rep.Hotspot.HotOps {
		t.Errorf("hot range ops = %d, want %d", ops, 2*rep.Hotspot.HotOps)
	}
}
