//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
	"github.com/pboros/fdb-profile-analyzer/internal/sink"
)

// TestShardedTopologySpreadsData is the sharded bed's own test; the rest of
// the suite is deliberately topology-blind (it talks to the initiator's
// Distributed tables and cannot tell one shard from two — that blindness is
// the point of the unified schema). What the blind suite cannot catch is a
// mis-registered cluster where the Distributed layer quietly routes everything
// to one shard: every query still works, every count still adds up, and the
// second shard is an expensive space heater. This test catches that.
//
// Armed by CLICKHOUSE2_ADDRESS, which `make integration_test SHARDED=1` sets;
// without it (the single-shard bed) it skips.
func TestShardedTopologySpreadsData(t *testing.T) {
	ch2 := os.Getenv("CLICKHOUSE2_ADDRESS")
	if ch2 == "" {
		t.Skip("single-shard topology (CLICKHOUSE2_ADDRESS unset)")
	}
	ctx := context.Background()
	conn := clickhouse_(t)

	// The initiator must actually believe it has two shards, or the rest of
	// this test would pass vacuously against a stale cluster definition.
	shards := scalar[uint64](t, conn,
		"SELECT uniqExact(shard_num) FROM system.clusters WHERE cluster = 'fdb'")
	if shards < 2 {
		t.Fatalf("cluster 'fdb' has %d shard(s) on the initiator; SHARDED=1 should give 2", shards)
	}

	// Its own workload, so the per-shard assertions do not depend on what the
	// rest of the suite happened to generate.
	pfx := testPfx + "sh"
	rep := generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.Prefixes = []string{pfx}
		c.HotOps, c.WarmOps, c.ColdKeys = 10, 4, 20
	})
	ingestUntil(t, []string{pfx}, 90*time.Second, func() error {
		got := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", rep.Hotspot.HotKey)
		if int(got) != rep.Hotspot.HotOps {
			return fmt.Errorf("hot key reads = %d, want %d", got, rep.Hotspot.HotOps)
		}
		return nil
	})

	// A second connection straight to shard 2's own port, bypassing the
	// Distributed layer entirely.
	cfg := chConfig()
	cfg.Address = ch2
	conn2, err := sink.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("connecting to shard 2 at %s: %v", ch2, err)
	}
	defer conn2.Close()

	for _, table := range []string{"operations", "mutations", "conflict_ranges"} {
		dist := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+"."+table+" WHERE key_prefix = ?", pfx)
		local1 := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+"."+table+"_local WHERE key_prefix = ?", pfx)
		local2 := scalar[uint64](t, conn2,
			"SELECT count() FROM "+chDB+"."+table+"_local WHERE key_prefix = ?", pfx)

		// Conservation: the Distributed view is exactly the union of the
		// shards — nothing duplicated by the routing, nothing dropped by it.
		if dist != local1+local2 {
			t.Errorf("%s: distributed count %d != shard1 %d + shard2 %d", table, dist, local1, local2)
		}
		// Spread: with dozens of distinct transaction ids hashed across two
		// shards, both must hold rows. All rows on one shard means the
		// sharding key or the cluster definition is broken, and a single-node
		// bed would never notice.
		if local1 == 0 || local2 == 0 {
			t.Errorf("%s: rows did not spread (shard1 %d, shard2 %d)", table, local1, local2)
		}
	}

	// The cursor: hashed by ingester_id, so one ingester's history lives whole
	// on one shard — and exactly one, or the max-version read could race a
	// stale row on another shard.
	curDist := scalar[uint64](t, conn, "SELECT count() FROM "+chDB+".ingest_cursor")
	cur1 := scalar[uint64](t, conn, "SELECT count() FROM "+chDB+".ingest_cursor_local")
	cur2 := scalar[uint64](t, conn2, "SELECT count() FROM "+chDB+".ingest_cursor_local")
	if curDist != cur1+cur2 {
		t.Errorf("ingest_cursor: distributed %d != %d + %d", curDist, cur1, cur2)
	}
}
