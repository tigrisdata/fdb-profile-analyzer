//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
)

// TestReplayedBatchDeduplicates pins the claim every table comment leans on:
// delivery is at-least-once — a crash between the insert and the cursor save
// replays a batch — and the ReplacingMergeTree sort keys collapse the replay.
// If a refactor ever drops a uniqueness column from a sort key, this is the
// test that notices, and the failure it prevents is permanent double-counting
// on every dashboard panel after any daemon crash.
func TestReplayedBatchDeduplicates(t *testing.T) {
	pfx := testPfx + "rp"
	conn := clickhouse_(t)

	// Drain whatever the suite has produced so far, so c0 is a cursor from
	// just before this test's own batch — the replay then re-reads exactly
	// this batch, classified with exactly the same prefix list, which is what
	// makes the replayed rows byte-identical in every sort-key column.
	c0, _, _ := ingest(t, nil, []string{pfx})

	rep := generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.Prefixes = []string{pfx}
		c.HotOps, c.WarmOps, c.ColdKeys = 8, 3, 3
	})

	ingestUntil(t, []string{pfx}, 90*time.Second, func() error {
		got := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", rep.Hotspot.HotKey)
		if int(got) != rep.Hotspot.HotOps {
			return fmt.Errorf("hot key reads = %d, want %d", got, rep.Hotspot.HotOps)
		}
		return nil
	})

	// FINAL forces replace-merge semantics at read time, so the counts are
	// exact rather than eventually-exact — this test is about identity, not
	// merge scheduling.
	counts := func() (ops, muts, crs uint64) {
		ops = scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations FINAL WHERE key_prefix = ?", pfx)
		muts = scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".mutations FINAL WHERE key_prefix = ?", pfx)
		crs = scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".conflict_ranges FINAL WHERE key_prefix = ?", pfx)
		return
	}
	ops1, muts1, crs1 := counts()
	if ops1 == 0 || muts1 == 0 {
		t.Fatalf("batch produced no rows (ops=%d muts=%d)", ops1, muts1)
	}

	// The crash-replay: ingest the same span again from the pre-batch cursor,
	// as a daemon restarting from an unsaved cursor would.
	ingest(t, c0, []string{pfx})

	ops2, muts2, crs2 := counts()
	if ops1 != ops2 || muts1 != muts2 || crs1 != crs2 {
		t.Errorf("replay changed the deduplicated counts: ops %d→%d, mutations %d→%d, conflict_ranges %d→%d",
			ops1, ops2, muts1, muts2, crs1, crs2)
	}
}
