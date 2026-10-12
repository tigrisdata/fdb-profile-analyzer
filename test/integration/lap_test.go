//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
	"github.com/pboros/fdb-profile-analyzer/internal/profile"
	"github.com/pboros/fdb-profile-analyzer/internal/tail"
)

// TestRingBufferLapIsVisible pins the pipeline's core operational contract:
// losing profile history is allowed — the tool never pushes back on the
// cluster — but silent loss is a bug. It reproduces the loss (shrink the ring
// buffer, write a batch nobody ingests, push it out with big commits) and then
// asserts every link of the visibility chain: the tailer detects the lap, the
// daemon records it to the series the dashboard annotates, and the trimmed
// batch is demonstrably absent from ClickHouse.
//
// Gated behind LAP_TEST=1 and run as the *last* step of `make
// integration_test`: it retunes the cluster-wide profile buffer, and the new
// size reaches clients asynchronously, so running it mid-suite would race
// every other test's exact-count assertions.
func TestRingBufferLapIsVisible(t *testing.T) {
	if os.Getenv("LAP_TEST") == "" {
		t.Skip("set LAP_TEST=1 to run; it shrinks the cluster's profile buffer and must run last")
	}
	ctx := context.Background()
	conn := clickhouse_(t)

	// Whatever happens, put the buffer back and wait until a fresh read agrees,
	// so nothing after us inherits a 500KB cluster.
	t.Cleanup(func() { setProfileBuffer(t, "100MB", 100_000_000) })

	// --- 1. Batch A: ingested normally; its final cursor is the position the
	// lap will be measured against.
	pfxA, pfxB, pfxBig := testPfx+"lapA", testPfx+"lapB", testPfx+"lapBig"
	generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.Prefixes = []string{pfxA}
		c.HotOps, c.WarmOps, c.ColdKeys = 6, 2, 2
	})
	cursor, _, _ := ingest(t, nil, []string{pfxA})
	if cursor == nil {
		t.Fatal("ingesting batch A produced no cursor")
	}

	// --- 2. Batch B: written, never ingested. Note the newest version in the
	// keyspace afterwards: batch B lives at or below it.
	repB := generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.Prefixes = []string{pfxB}
		c.HotOps, c.WarmOps, c.ColdKeys = 6, 2, 2
	})
	vB, ok := edgeVersion(t, false)
	if !ok {
		t.Fatal("no profile keys after generating batch B")
	}

	// --- 3. Shrink the buffer and push batch B out of it with big commits.
	setProfileBuffer(t, "500KB", 500_000)
	trimmed := false
	for attempt := 1; attempt <= 8; attempt++ {
		if _, err := loadgen.Run(db, loadgen.ModeBig, bigCfg(pfxBig)); err != nil {
			t.Fatalf("big load %d: %v", attempt, err)
		}
		if oldest, ok := edgeVersion(t, true); ok && oldest > vB {
			trimmed = true
			break
		}
	}
	if !trimmed {
		t.Fatal("ring buffer never trimmed past batch B; is the size limit propagating?")
	}

	// --- 4. Resume from the stored position: the tailer must detect the lap,
	// and ingestion must carry on regardless — shedding is the design.
	tailer := tail.New(db, tail.Options{Cursor: cursor})
	w := writer(t, []string{pfxA, pfxB, pfxBig})
	sawLap, sawResume := false, false
	for {
		res, err := tailer.Poll(10000)
		if err != nil {
			t.Fatalf("polling across the lap: %v", err)
		}
		if res.Lapped {
			sawLap = true
			if res.Lap.OnResume {
				sawResume = true
			}
			if res.Lap.FromVersion == 0 || res.Lap.ToVersion <= res.Lap.FromVersion {
				t.Errorf("lap versions not ordered: %+v", res.Lap)
			}
		}
		if res.KeysRead == 0 {
			break
		}
		if _, err := w.Write(ctx, res.Records, res.Profiling); err != nil {
			t.Fatalf("writing survivors: %v", err)
		}
	}
	if !sawLap || !sawResume {
		t.Fatalf("lap not detected on resume (lap=%v resume=%v)", sawLap, sawResume)
	}
	if got := tailer.Stats().Laps; got == 0 {
		t.Error("Stats.Laps = 0 after a detected lap")
	}

	// --- 5. The loss itself: batch B must be absent — that is the trade — and
	// batch A intact.
	gotB := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".operations WHERE key = ?", repB.Hotspot.HotKey)
	if gotB != 0 {
		t.Errorf("batch B hot key has %d rows; expected it trimmed unread", gotB)
	}
	gotA := scalar[uint64](t, conn,
		"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", pfxA+"/hot/key")
	if gotA == 0 {
		t.Error("batch A vanished from ClickHouse; the lap should only have cost unread records")
	}

	// --- 6. The daemon-level visibility chain: give the real daemon a cursor
	// from before the hole and run it once. It must warn, and it must land a
	// row the dashboard's annotation query picks up.
	probeID := "lap-probe-" + runID
	if err := w.SaveCursor(ctx, probeID, cursor, 0, true); err != nil {
		t.Fatalf("saving probe cursor: %v", err)
	}
	out, err := runDaemonOnce(t, probeID)
	if err != nil {
		t.Fatalf("daemon -once across the lap failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "history gap") {
		t.Errorf("daemon log does not warn about the gap:\n%s", tailOf(out, 15))
	}
	gaps := scalar[uint64](t, conn,
		"SELECT sum(laps + resume_gaps) FROM "+chDB+".profiling_config WHERE ingester_id = ?", probeID)
	if gaps == 0 {
		t.Error("no loss event in profiling_config; the dashboard would show nothing")
	}
}

// setProfileBuffer retunes the cluster's profile buffer via the host fdbcli —
// the same tool an operator would use — and waits until a fresh read agrees.
func setProfileBuffer(t *testing.T, human string, want int64) {
	t.Helper()
	cli := envOr("FDBCLI", "fdbcli")
	clusterFile := envOr("FDB_CLUSTER_FILE", findClusterFile())
	out, err := exec.Command(cli, "-C", clusterFile,
		"--exec", fmt.Sprintf("profile client set 1.0 %s", human), "--timeout", "20").CombinedOutput()
	if err != nil {
		t.Fatalf("fdbcli profile client set %s: %v\n%s", human, err, out)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		p, err := fdbconfig.Read(db)
		if err == nil && p.SizeLimit == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("size limit %d not visible after 30s (last: %+v, err %v)", want, p, err)
		}
		time.Sleep(time.Second)
	}
}

// edgeVersion returns the commit version of the oldest (first=true) or newest
// key currently in the profile keyspace.
func edgeVersion(t *testing.T, first bool) (uint64, bool) {
	t.Helper()
	kv, err := fdbconfig.SystemRead(db, func(tr fdb.ReadTransaction) ([]fdb.KeyValue, error) {
		return tr.GetRange(
			fdb.KeyRange{Begin: fdb.Key(profile.KeyPrefix), End: fdb.Key(profile.EndKey)},
			fdb.RangeOptions{Limit: 1, Reverse: !first},
		).GetSliceWithError()
	})
	if err != nil {
		t.Fatalf("reading keyspace edge: %v", err)
	}
	if len(kv) == 0 {
		return 0, false
	}
	k, err := profile.ParseKey(kv[0].Key)
	if err != nil {
		t.Fatalf("parsing edge key: %v", err)
	}
	return k.Version, true
}

func bigCfg(pfx string) loadgen.Config {
	cfg := loadgen.Default()
	cfg.Prefixes = []string{pfx}
	cfg.Warmup = 0
	cfg.FlushWait = 12 * time.Second
	return cfg
}

// runDaemonOnce runs the actual fdbprof binary in -once mode from the repo
// root, so the test exercises the shipped wiring — LoadCursor, lap warning,
// immediate profiling_config write — not a reimplementation of it.
func runDaemonOnce(t *testing.T, ingesterID string) (string, error) {
	t.Helper()
	cmd := exec.Command("go", "run", "./cmd/fdbprof", "-once", "-ingester-id", ingesterID)
	cmd.Dir = repoPath(t, ".")
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func tailOf(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
