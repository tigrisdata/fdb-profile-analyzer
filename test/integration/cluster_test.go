//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
	"github.com/pboros/fdb-profile-analyzer/internal/tail"
)

// clusterGuard is the dashboard's cluster filter, verbatim — the same
// sentinel-allValue shape every other variable uses. Stated here so the test
// exercises the exact predicate the panels run, not a paraphrase of it;
// TestDashboardQueriesExecute pins that every shipped query contains it.
const clusterGuard = `(cluster IN (%s) OR '__all__' IN (%s))`

// TestClusterIsolation pins the multi-cluster contract: in production several
// FoundationDB clusters feed one ClickHouse, and a query filtered to one
// cluster must see exactly that cluster's rows. It ships the same span of
// records twice under two labels — as an agent for a second cluster would —
// and asserts the guard separates them, and that the sentinel deliberately
// does not.
func TestClusterIsolation(t *testing.T) {
	ctx := context.Background()
	conn := clickhouse_(t)
	const other = "other-fdb"

	// One workload, ingested normally under the test bed's cluster label.
	pfx := testPfx + "iso"
	rep := generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.Prefixes = []string{pfx}
		c.HotOps, c.WarmOps, c.ColdKeys = 8, 3, 2
	})
	h := rep.Hotspot
	var cursor []byte
	ingestUntil(t, []string{pfx}, 90*time.Second, func() error {
		got := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", h.HotKey)
		if int(got) != h.HotOps {
			return fmt.Errorf("hot key reads = %d, want %d", got, h.HotOps)
		}
		return nil
	})

	// The same records again, shipped by a writer configured as a different
	// cluster's agent. Same events, same keys, different label — the closest
	// thing to two identical clusters reporting into one ClickHouse, and the
	// worst case for a filter: nothing but the label tells the rows apart.
	w2 := writerFor(t, []string{pfx}, other)
	tailer := tail.New(db, tail.Options{})
	for {
		res, err := tailer.Poll(10000)
		if err != nil {
			t.Fatalf("polling: %v", err)
		}
		if res.KeysRead == 0 {
			break
		}
		if _, err := w2.Write(ctx, res.Records, res.Profiling); err != nil {
			t.Fatalf("writing as %q: %v", other, err)
		}
		cursor = tailer.Cursor()
	}

	count := func(guardArg string) uint64 {
		g := fmt.Sprintf(clusterGuard, guardArg, guardArg)
		return scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ? AND "+g, h.HotKey)
	}
	clusters := func(guardArg string) uint64 {
		g := fmt.Sprintf(clusterGuard, guardArg, guardArg)
		return scalar[uint64](t, conn,
			"SELECT uniqExact(cluster) FROM "+chDB+".operations WHERE key = ? AND "+g, h.HotKey)
	}

	// Both labels hold the full workload: the duplicates deliberately did NOT
	// collapse, because cluster leads every sort key — rows from different
	// clusters are different facts, not replays of one another.
	for _, c := range []string{testCluster(), other} {
		if got := count("'" + c + "'"); int(got) != h.HotOps {
			t.Errorf("cluster %q: hot key reads = %d, want %d", c, got, h.HotOps)
		}
	}

	// The isolation itself: one cluster selected, exactly one cluster's rows.
	if got := clusters("'" + testCluster() + "'"); got != 1 {
		t.Errorf("guard for one cluster matched %d clusters, want 1", got)
	}
	// And the sentinel — Grafana's "All" — sees both, so selecting everything
	// remains possible rather than accidentally pinned to one.
	if got := clusters("'__all__'"); got != 2 {
		t.Errorf("'__all__' sentinel matched %d clusters, want 2", got)
	}

	// The mutations table gets the same treatment: the hot-write-key panel is
	// just as blendable.
	mutOne := scalar[uint64](t, conn,
		"SELECT uniqExact(cluster) FROM "+chDB+".mutations WHERE key = ? AND "+
			fmt.Sprintf(clusterGuard, "'"+other+"'", "'"+other+"'"), h.HotKey)
	if mutOne != 1 {
		t.Errorf("mutations guard matched %d clusters, want 1", mutOne)
	}

	// Cursor isolation: two agents on different clusters may share a
	// hostname-derived ingester id; each must get its own position back. The
	// second cluster's cursor is deliberately different (a bumped version
	// byte) so a cross-read is distinguishable from a coincidence.
	if cursor == nil {
		t.Fatal("no cursor from the second ingest")
	}
	ingesterID := "iso-" + runID
	cursorB := append([]byte(nil), cursor...)
	cursorB[39]++ // low byte of the big-endian commit version
	w1 := writer(t, []string{pfx})
	if err := w1.SaveCursor(ctx, ingesterID, cursor, 1, true); err != nil {
		t.Fatalf("saving cursor as %q: %v", testCluster(), err)
	}
	if err := w2.SaveCursor(ctx, ingesterID, cursorB, 1, true); err != nil {
		t.Fatalf("saving cursor as %q: %v", other, err)
	}
	got1, _, err := w1.LoadCursor(ctx, ingesterID)
	if err != nil {
		t.Fatalf("loading cursor as %q: %v", testCluster(), err)
	}
	got2, _, err := w2.LoadCursor(ctx, ingesterID)
	if err != nil {
		t.Fatalf("loading cursor as %q: %v", other, err)
	}
	if !bytes.Equal(got1, cursor) || !bytes.Equal(got2, cursorB) {
		t.Errorf("cursors crossed clusters: cluster %q got its own = %v, %q got its own = %v",
			testCluster(), bytes.Equal(got1, cursor), other, bytes.Equal(got2, cursorB))
	}
}
