//go:build integration

// Package integration exercises the whole pipeline against a live test bed:
// generate profiled traffic, tail it out of FoundationDB, decode it, write it
// to ClickHouse, and assert on what lands.
//
// Run with `make integration_test`, which brings the containers up, runs these,
// records the results and tears everything down.
package integration

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/pboros/fdb-profile-analyzer/internal/config"
	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
	"github.com/pboros/fdb-profile-analyzer/internal/prefix"
	"github.com/pboros/fdb-profile-analyzer/internal/sink"
	"github.com/pboros/fdb-profile-analyzer/internal/tail"
)

var (
	db      fdb.Database
	chDB    string
	runID   string
	testPfx string
)

func TestMain(m *testing.M) {
	fdb.MustAPIVersion(config.DefaultAPIVersion)

	clusterFile := envOr("FDB_CLUSTER_FILE", findClusterFile())
	var err error
	db, err = fdb.OpenDatabase(clusterFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open FoundationDB (cluster file %q): %v\n", clusterFile, err)
		fmt.Fprintln(os.Stderr, "is the test bed up? try: make testenv_up")
		os.Exit(1)
	}

	chDB = envOr("CLICKHOUSE_DB", "fdb_profiles")

	// Every run uses its own key prefix, so repeated runs and parallel tests
	// cannot see each other's rows.
	runID = fmt.Sprintf("%d", time.Now().UnixNano())
	testPfx = "/it" + runID

	if err := awaitSampling(); err != nil {
		fmt.Fprintf(os.Stderr, "profiling is not producing records: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// awaitSampling blocks until this client is demonstrably being profiled.
//
// Sampling reads the rate from global config, which the client fetches
// asynchronously and does not wait for. Until it arrives the rate falls back to
// CSI_SAMPLING_PROBABILITY (-1.0) and nothing is sampled — so the first
// transactions a fresh process runs are silently unprofiled. Rather than guess
// a sleep, drive a probe workload and wait for its record to actually appear.
func awaitSampling() error {
	probe := "/itprobe" + runID
	deadline := time.Now().Add(90 * time.Second)

	tailer := tail.New(db, tail.Options{})
	for attempt := 1; ; attempt++ {
		cfg := loadgen.Default()
		cfg.Prefixes = []string{probe}
		cfg.Warmup = 0
		cfg.FlushWait = 12 * time.Second
		cfg.HotOps, cfg.WarmOps, cfg.ColdKeys = 2, 1, 1
		if _, err := loadgen.Run(db, loadgen.ModeHotspot, cfg); err != nil {
			return fmt.Errorf("probe workload: %w", err)
		}

		for {
			res, err := tailer.Poll(10000)
			if err != nil {
				return fmt.Errorf("polling: %w", err)
			}
			if res.KeysRead == 0 {
				break
			}
			for _, rec := range res.Records {
				for _, ev := range rec.Events {
					if strings.HasPrefix(ev.Key, probe) {
						return nil
					}
				}
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("no profile record for the probe workload after %d attempts; "+
				"is client profiling enabled? (fdbcli> profile client set 1.0 100MB)", attempt)
		}
	}
}

func envOr(key, fallback string) string { return cmp.Or(os.Getenv(key), fallback) }

// findClusterFile walks up from the test's working directory looking for the
// test bed's cluster file, which make writes at the repo root. Without this,
// running `go test -tags=integration ./test/...` directly would resolve
// ".testenv/fdb.cluster" relative to test/integration/ and never find it.
func findClusterFile() string {
	dir, err := os.Getwd()
	if err != nil {
		return ".testenv/fdb.cluster"
	}
	for i := 0; i < 5; i++ {
		candidate := filepath.Join(dir, ".testenv", "fdb.cluster")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ".testenv/fdb.cluster"
}

func clickhouse_(t *testing.T) driver.Conn {
	t.Helper()
	conn, err := sink.Open(context.Background(), chConfig())
	if err != nil {
		t.Fatalf("connecting to ClickHouse: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func chConfig() config.ClickHouse {
	cfg := config.Default().ClickHouse
	cfg.Address = envOr("CLICKHOUSE_ADDRESS", cfg.Address)
	cfg.Database = chDB
	cfg.Username = envOr("CLICKHOUSE_USER", cfg.Username)
	cfg.Password = envOr("CLICKHOUSE_PASSWORD", cfg.Password)
	return cfg
}

// ingest tails everything currently in the profile keyspace and writes it,
// returning the cursor so a caller can test resumption.
func ingest(t *testing.T, cursor []byte, prefixes []string) ([]byte, sink.Counts, int) {
	t.Helper()
	return ingestAs(t, cursor, prefixes, nil)
}

// ingestAs is ingest with control over the profiling setting stamped on the
// rows. A nil override means "whatever the tailer read from the cluster",
// which is what the daemon does; passing one lets a test pin a sample rate the
// test bed is not actually running at.
func ingestAs(t *testing.T, cursor []byte, prefixes []string, override *fdbconfig.Profiling) ([]byte, sink.Counts, int) {
	t.Helper()
	ctx := context.Background()

	cfg := config.Default()
	cfg.ClickHouse = chConfig()

	w, err := sink.New(ctx, cfg.ClickHouse, cfg.Ingest, prefix.New(prefixes), testCluster())
	if err != nil {
		t.Fatalf("creating writer: %v", err)
	}
	defer w.Close()

	tailer := tail.New(db, tail.Options{Cursor: cursor, MaxPartials: cfg.FDB.MaxPartials})

	var total sink.Counts
	records := 0
	for {
		res, err := tailer.Poll(cfg.FDB.BatchSize)
		if err != nil {
			t.Fatalf("polling: %v", err)
		}
		if res.KeysRead == 0 {
			break
		}
		if len(res.Records) == 0 {
			continue
		}
		records += len(res.Records)
		prof := res.Profiling
		if override != nil {
			prof = *override
		}
		counts, err := w.Write(ctx, res.Records, prof)
		if err != nil {
			t.Fatalf("writing: %v", err)
		}
		total.Operations += counts.Operations
		total.Mutations += counts.Mutations
		total.ConflictRanges += counts.ConflictRanges
		total.SkippedSystem += counts.SkippedSystem
	}

	if st := tailer.Stats(); st.DecodeErrors > 0 {
		t.Errorf("decoder reported %d errors over %d keys", st.DecodeErrors, st.KeysRead)
	}
	// Nothing in the ordinary suite trims the 100MB test buffer, so any lap
	// here is a false positive in the detector — the failure mode that would
	// train operators to ignore the real thing.
	if st := tailer.Stats(); st.Laps > 0 {
		t.Errorf("lap detector fired %d time(s) during a normal ingest", st.Laps)
	}
	return tailer.Cursor(), total, records
}

// ingestUntil tails and writes repeatedly until check passes, or the budget
// runs out.
//
// The pipeline is asynchronous at two points that no fixed sleep can paper
// over: the client queues a profile record when the transaction is destroyed,
// which in Go waits on a GC finalizer, and the client then flushes its queue
// only every CSI_STATUS_DELAY (10s). Polling for the expected state is both
// more honest and far less flaky than guessing a duration.
func ingestUntil(t *testing.T, prefixes []string, budget time.Duration, check func() error) {
	t.Helper()
	ingestUntilAs(t, prefixes, nil, budget, check)
}

// ingestUntilAs is ingestUntil with the same override as ingestAs.
func ingestUntilAs(t *testing.T, prefixes []string, override *fdbconfig.Profiling, budget time.Duration, check func() error) {
	t.Helper()

	deadline := time.Now().Add(budget)
	var cursor []byte
	var last error
	for attempt := 1; ; attempt++ {
		cursor, _, _ = ingestAs(t, cursor, prefixes, override)
		if last = check(); last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s (%d attempts): %v", budget, attempt, last)
		}
		time.Sleep(2 * time.Second)
	}
}

// generate runs a workload and waits for the profiles to reach the cluster.
func generate(t *testing.T, mode string, tune func(*loadgen.Config)) *loadgen.Report {
	t.Helper()

	cfg := loadgen.Default()
	cfg.Prefixes = []string{testPfx}
	cfg.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	// Global config has long since reached this process by the time tests run.
	cfg.Warmup = 0
	cfg.FlushWait = 12 * time.Second
	if tune != nil {
		tune(&cfg)
	}

	rep, err := loadgen.Run(db, mode, cfg)
	if err != nil {
		t.Fatalf("generating %s load: %v", mode, err)
	}
	return rep
}

// writer returns a sink.Writer dialled exactly as the daemon dials it,
// labelling rows with the test bed's cluster name.
func writer(t *testing.T, prefixes []string) *sink.Writer {
	t.Helper()
	return writerFor(t, prefixes, testCluster())
}

// writerFor is writer with an explicit cluster label, for tests that simulate
// a second FoundationDB cluster feeding the same ClickHouse.
func writerFor(t *testing.T, prefixes []string, cluster string) *sink.Writer {
	t.Helper()
	cfg := config.Default()
	cfg.ClickHouse = chConfig()
	w, err := sink.New(context.Background(), cfg.ClickHouse, cfg.Ingest, prefix.New(prefixes), cluster)
	if err != nil {
		t.Fatalf("creating writer: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// testCluster is the label the test bed's agent runs under.
func testCluster() string { return envOr("FDB_CLUSTER_NAME", "test") }

// repoPath resolves a repo-relative path from wherever `go test` happens to be
// run, by walking up to the module root. Same problem findClusterFile solves,
// and the same shape of answer.
func repoPath(t *testing.T, rel string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for range 6 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, rel)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("could not find the module root above %s", dir)
	return ""
}

func scalar[T any](t *testing.T, conn driver.Conn, query string, args ...any) T {
	t.Helper()
	var v T
	if err := conn.QueryRow(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}
