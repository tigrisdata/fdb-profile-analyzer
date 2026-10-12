// Package loadgen generates profiled FoundationDB traffic.
//
// It is a real FoundationDB client rather than an fdbcli script, and it has to
// be: fdbcli calls setNetworkOption(DISABLE_CLIENT_STATISTICS_LOGGING) at
// startup, so its transactions are never sampled whatever the cluster's
// profiling rate says.
package loadgen

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Modes supported by Run.
const (
	ModeBasic    = "basic"
	ModeBig      = "big"
	ModeConflict = "conflict"
	ModeHotspot  = "hotspot"
	ModeAll      = "all"
	// ModeContinuous loops scaled-down mixes of the other modes until the
	// context is cancelled — the workload behind `make load_start`.
	ModeContinuous = "continuous"
)

// value is 40 bytes of filler, so mutation sizes are predictable.
var value = []byte(strings.Repeat("v", 40))

// Config describes the workload.
type Config struct {
	Prefixes []string

	Txns       int // basic: transactions per prefix
	SetsPerTxn int // basic: sets per transaction
	BigSets    int // big: mutations in the single large transaction
	HotOps     int // hotspot: operations on the hot key
	WarmOps    int // hotspot: operations on the warm key
	ColdKeys   int // hotspot: distinct cold keys, one operation each

	// Warmup lets global config reach the client before any work starts.
	// Sampling reads the profiling rate without waiting for it, and until it
	// arrives the rate falls back to CSI_SAMPLING_PROBABILITY (-1.0), which
	// samples nothing.
	Warmup time.Duration
	// FlushWait keeps the process alive afterwards so the client's profile
	// buffer is flushed; clients flush every CSI_STATUS_DELAY (10s).
	FlushWait time.Duration
	// Interval is the pause between continuous-mode cycles.
	Interval time.Duration

	// Logf receives progress messages. Nil discards them.
	Logf func(format string, args ...any)
}

// Default returns a workload sized for the test bed.
func Default() Config {
	return Config{
		Prefixes:   []string{"/users", "/orders", "/events"},
		Txns:       8,
		SetsPerTxn: 5,
		BigSets:    1500,
		HotOps:     60,
		WarmOps:    20,
		ColdKeys:   60,
		Warmup:     5 * time.Second,
		FlushWait:  15 * time.Second,
		Interval:   5 * time.Second,
	}
}

func (c Config) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

func (c Config) prefix() string {
	if len(c.Prefixes) == 0 {
		return "/loadgen"
	}
	return c.Prefixes[0]
}

// Hotspot describes the skew the hotspot mode produces. Counts are exact, so
// tests can assert on the resulting ranking rather than eyeballing a chart.
type Hotspot struct {
	HotKey   string
	WarmKey  string
	HotOps   int
	WarmOps  int
	ColdKeys int
	RangeKey string
	RangeEnd string
	RangeOps int
	TotalOps int
}

// Report is what a run produced.
type Report struct {
	Commits  int
	Hotspot  *Hotspot
	BigSets  int
	Conflict bool
}

// Run executes the workload. It returns once the work is done and, if
// FlushWait is set, once the client has had time to flush its profiles.
func Run(db fdb.Database, mode string, cfg Config) (*Report, error) {
	if cfg.Warmup > 0 {
		cfg.logf("waiting %s for global config to reach the client", cfg.Warmup)
		time.Sleep(cfg.Warmup)
	}

	rep := &Report{}
	sub := cfg
	sub.Warmup, sub.FlushWait = 0, 0

	var err error
	switch mode {
	case ModeBasic:
		err = runBasic(db, cfg, rep)
	case ModeBig:
		err = runBig(db, cfg, rep)
	case ModeConflict:
		err = runConflict(db, cfg, rep)
	case ModeHotspot:
		err = runHotspot(db, cfg, rep)
	case ModeAll:
		for _, m := range []string{ModeBasic, ModeBig, ModeConflict, ModeHotspot} {
			r, e := Run(db, m, sub)
			if e != nil {
				return rep, fmt.Errorf("%s: %w", m, e)
			}
			rep.Commits += r.Commits
			if r.Hotspot != nil {
				rep.Hotspot = r.Hotspot
			}
			if r.BigSets > 0 {
				rep.BigSets = r.BigSets
			}
			rep.Conflict = rep.Conflict || r.Conflict
		}
	default:
		return nil, fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		return rep, err
	}

	if cfg.FlushWait > 0 {
		flushProfiles(cfg.FlushWait, cfg.Logf)
	}
	return rep, nil
}

// flushProfiles makes the client hand its buffered profile records to the
// cluster.
//
// A record is queued by Transaction::~Transaction in the C client, and the Go
// bindings destroy a transaction only from a runtime.SetFinalizer callback with
// no explicit close. With a small heap the collector may never run, so without
// forcing it no records are queued at all — which is why a Go service with low
// GC pressure silently under-reports its profiles.
func flushProfiles(wait time.Duration, logf func(string, ...any)) {
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
	}
	if logf != nil {
		logf("waiting %s for the profile flush", wait)
	}
	time.Sleep(wait)
}

// runBasic exercises every non-error event type across all prefixes.
func runBasic(db fdb.Database, cfg Config, rep *Report) error {
	cfg.logf("basic: %d transactions per prefix, %d sets each, prefixes %v",
		cfg.Txns, cfg.SetsPerTxn, cfg.Prefixes)

	n := 0
	for _, prefix := range cfg.Prefixes {
		for i := 0; i < cfg.Txns; i++ {
			n++
			seq := n
			if _, err := db.Transact(func(tr fdb.Transaction) (any, error) {
				// Read BEFORE writing, and never a key this transaction has
				// set: a get of a key already written in the same transaction
				// is served from the read-your-writes cache and produces no
				// get event at all.
				tr.Get(fdb.Key(fmt.Sprintf("%s/k%06d", prefix, seq*100))).MustGet()
				for s := 0; s < cfg.SetsPerTxn; s++ {
					tr.Set(fdb.Key(fmt.Sprintf("%s/k%06d", prefix, seq*100+s)), value)
				}
				tr.GetRange(
					fdb.KeyRange{Begin: fdb.Key(prefix + "/k"), End: fdb.Key(prefix + "/l")},
					fdb.RangeOptions{Limit: 25},
				).GetSliceOrPanic()
				return nil, nil
			}); err != nil {
				return err
			}
			rep.Commits++
		}

		// Read-only: get_version and get_range, no commit.
		if _, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
			tr.GetRange(
				fdb.KeyRange{Begin: fdb.Key(prefix + "/"), End: fdb.Key(prefix + "0")},
				fdb.RangeOptions{Limit: 50},
			).GetSliceOrPanic()
			return nil, nil
		}); err != nil {
			return err
		}

		// Clears, so commits carry CLEAR_RANGE mutations too. The single-key
		// tr.Clear matters: FoundationDB serializes it in an optimized wire
		// form the decoder has to reconstruct, so the test bed must produce it.
		if _, err := db.Transact(func(tr fdb.Transaction) (any, error) {
			tr.ClearRange(fdb.KeyRange{
				Begin: fdb.Key(prefix + "/k000001"),
				End:   fdb.Key(prefix + "/k000002"),
			})
			tr.Clear(fdb.Key(prefix + "/k000003"))
			return nil, nil
		}); err != nil {
			return err
		}
		rep.Commits++
	}
	return nil
}

// runBig commits enough mutations that the profile record exceeds the 100KB
// value limit and is split across chunks, exercising reassembly.
func runBig(db fdb.Database, cfg Config, rep *Report) error {
	prefix := cfg.prefix()
	cfg.logf("big: one transaction with %d mutations under %s", cfg.BigSets, prefix)

	if _, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		for i := 0; i < cfg.BigSets; i++ {
			tr.Set(fdb.Key(fmt.Sprintf("%s/big/%030d", prefix, i)), value)
		}
		return nil, nil
	}); err != nil {
		return err
	}
	rep.Commits++
	rep.BigSets = cfg.BigSets
	return nil
}

// runConflict makes one transaction lose a write conflict, producing an
// error_commit event. Raw transactions, not Transact, so the loser is not
// retried before the failure is observed.
func runConflict(db fdb.Database, cfg Config, rep *Report) error {
	key := fdb.Key(cfg.prefix() + "/conflict/k")
	cfg.logf("conflict: racing two transactions on %s", key)

	loser, err := db.CreateTransaction()
	if err != nil {
		return err
	}
	// A read to establish the conflict range. This is a raw transaction with
	// no retry wrapper, so errors return rather than MustGet-panic.
	if _, err := loser.Get(key).Get(); err != nil {
		return fmt.Errorf("loser read: %w", err)
	}

	if _, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Get(key).MustGet()
		tr.Set(key, []byte("winner"))
		return nil, nil
	}); err != nil {
		return fmt.Errorf("winner: %w", err)
	}
	rep.Commits++

	loser.Set(key, []byte("loser"))
	if err := loser.Commit().Get(); err != nil {
		cfg.logf("loser conflicted as intended: %v", err)
		rep.Conflict = true
		return nil
	}
	cfg.logf("WARNING: the loser committed; no error_commit event was produced")
	return nil
}

// runHotspot creates a deliberate, known skew.
//
// Every operation gets its own transaction on purpose. A transaction is
// read-your-writes, so repeating a get or a set on one key inside a single
// transaction is served from the local cache and collapses into one mutation,
// producing no skew at all.
func runHotspot(db fdb.Database, cfg Config, rep *Report) error {
	prefix := cfg.prefix()
	hot := prefix + "/hot/key"
	warm := prefix + "/warm/key"

	cfg.logf("hotspot: hot=%d warm=%d cold=%d under %s",
		cfg.HotOps, cfg.WarmOps, cfg.ColdKeys, prefix)

	touch := func(k string) error {
		_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
			tr.Get(fdb.Key(k)).MustGet()
			tr.Set(fdb.Key(k), value)
			return nil, nil
		})
		if err == nil {
			rep.Commits++
		}
		return err
	}

	for i := 0; i < cfg.HotOps; i++ {
		if err := touch(hot); err != nil {
			return err
		}
	}
	for i := 0; i < cfg.WarmOps; i++ {
		if err := touch(warm); err != nil {
			return err
		}
	}
	for i := 0; i < cfg.ColdKeys; i++ {
		if err := touch(fmt.Sprintf("%s/cold/%06d", prefix, i)); err != nil {
			return err
		}
	}

	rangeBegin, rangeEnd := prefix+"/cold/", prefix+"/cold0"
	scans := cfg.HotOps / 2
	for i := 0; i < scans; i++ {
		if _, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
			tr.GetRange(
				fdb.KeyRange{Begin: fdb.Key(rangeBegin), End: fdb.Key(rangeEnd)},
				fdb.RangeOptions{Limit: 25},
			).GetSliceOrPanic()
			return nil, nil
		}); err != nil {
			return err
		}
	}

	rep.Hotspot = &Hotspot{
		HotKey: hot, WarmKey: warm,
		HotOps: cfg.HotOps, WarmOps: cfg.WarmOps, ColdKeys: cfg.ColdKeys,
		RangeKey: rangeBegin, RangeEnd: rangeEnd, RangeOps: scans,
		TotalOps: cfg.HotOps + cfg.WarmOps + cfg.ColdKeys,
	}
	return nil
}

// RunContinuous drives a modest, varied workload until ctx is cancelled: a
// scaled-down basic+hotspot mix every cycle, a chunked commit and a conflict
// every few cycles. The process's own 10-second profile flushes handle
// delivery; cycles only need to trigger the GC finalizers that queue records.
func RunContinuous(ctx context.Context, db fdb.Database, cfg Config) error {
	if cfg.Warmup > 0 {
		cfg.logf("waiting %s for global config to reach the client", cfg.Warmup)
		select {
		case <-time.After(cfg.Warmup):
		case <-ctx.Done():
			return nil
		}
	}

	// Scale the one-shot defaults down so a cycle is seconds, not minutes.
	cycleCfg := cfg
	cycleCfg.Warmup, cycleCfg.FlushWait = 0, 0
	cycleCfg.Txns, cycleCfg.SetsPerTxn = 2, 3
	cycleCfg.HotOps, cycleCfg.WarmOps, cycleCfg.ColdKeys = 10, 4, 8
	cycleCfg.BigSets = 400

	commits := 0
	for cycle := 1; ; cycle++ {
		modes := []string{ModeBasic, ModeHotspot}
		if cycle%5 == 0 {
			modes = append(modes, ModeConflict)
		}
		if cycle%10 == 0 {
			modes = append(modes, ModeBig)
		}

		for _, m := range modes {
			if ctx.Err() != nil {
				break
			}
			rep, err := Run(db, m, cycleCfg)
			if err != nil {
				// Keep generating through transient cluster errors; the point
				// of continuous load is that it stays up.
				cfg.logf("cycle %d %s: %v (continuing)", cycle, m, err)
				continue
			}
			commits += rep.Commits
		}

		// Nudge the GC so finished transactions are destroyed and their
		// profile records queued; the client flushes them on its own 10s
		// cadence while the process stays alive.
		runtime.GC()
		cfg.logf("cycle %d done (%d commits total)", cycle, commits)

		select {
		case <-ctx.Done():
			// One final flush window so the last cycle's records land.
			flushProfiles(12*time.Second, cfg.Logf)
			cfg.logf("stopped after %d cycles, %d commits", cycle, commits)
			return nil
		case <-time.After(cfg.Interval):
		}
	}
}

// Describe renders the expected hotspot shape.
func (h *Hotspot) Describe(logf func(string, ...any)) {
	if h == nil || logf == nil {
		return
	}
	pct := func(n int) float64 { return float64(n) * 100 / float64(h.TotalOps) }
	logf("expected shape (point reads, and SET mutations, alike):")
	logf("  %-28s %6d  %5.1f%%  rank 1", h.HotKey, h.HotOps, pct(h.HotOps))
	logf("  %-28s %6d  %5.1f%%  rank 2", h.WarmKey, h.WarmOps, pct(h.WarmOps))
	logf("  %-28s %6d  %5.1f%%  rank 3+",
		fmt.Sprintf("%s/cold/NNNNNN (x%d)", strings.TrimSuffix(h.HotKey, "/hot/key"), h.ColdKeys),
		1, pct(1))
	logf("  total point reads: %d", h.TotalOps)
	logf("  range %s .. %s scanned %d times", h.RangeKey, h.RangeEnd, h.RangeOps)
}
