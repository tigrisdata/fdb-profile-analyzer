// Command fdbprof tails FoundationDB's client transaction profiles and loads
// them into ClickHouse for analysis in Grafana.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/pboros/fdb-profile-analyzer/internal/build"
	"github.com/pboros/fdb-profile-analyzer/internal/config"
	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
	"github.com/pboros/fdb-profile-analyzer/internal/prefix"
	"github.com/pboros/fdb-profile-analyzer/internal/profile"
	"github.com/pboros/fdb-profile-analyzer/internal/sink"
	"github.com/pboros/fdb-profile-analyzer/internal/tail"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML configuration")
	ingesterID := flag.String("ingester-id", defaultIngesterID(), "identity for the stored cursor; distinct per daemon instance")
	once := flag.Bool("once", false, "drain what is available, then exit — useful in tests")
	verbose := flag.Bool("verbose", false, "log at debug level")
	version := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *version {
		fmt.Println("fdbprof", build.String())
		return
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	// A configuration that fails to load cannot pick its own log format, so
	// load-time errors report as text; everything after honours logging.format.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	// The default config path is optional — a containerised daemon typically
	// runs on defaults plus environment variables. An explicitly given path
	// must exist.
	path := *configPath
	if path == "config.yaml" {
		if _, err := os.Stat(path); err != nil {
			path = ""
		}
	}
	cfg, err := config.Load(path)
	if err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
	if cfg.Logging.Format == config.LogFormatJSON {
		log = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	}
	if path == "" && *configPath == "config.yaml" {
		log.Info("no config.yaml; using defaults and environment")
	}

	if err := run(log, cfg, path, *ingesterID, *once); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func defaultIngesterID() string {
	if v := os.Getenv("INGESTER_ID"); v != "" {
		return v
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "default"
}

func run(log *slog.Logger, cfg config.Config, configPath, ingesterID string, once bool) error {
	matcher := prefix.New(cfg.Prefixes)
	if len(matcher.Prefixes()) == 0 {
		// Legal, but rarely what anyone wants: every key falls back to a
		// first-16-bytes "(unknown)" label instead of the operator's own map
		// of the keyspace.
		log.Warn("no prefixes configured — every key will be labelled by its first 16 bytes as (unknown); set prefixes: in the config file")
	}
	log.Info("configuration loaded",
		"version", build.String(),
		"config", configPath,
		"cluster", cfg.FDB.ClusterName,
		"prefixes", matcher.Prefixes(),
		"capture_mutation_values", cfg.Ingest.CaptureMutationValues,
		"capture_conflict_ranges", cfg.Ingest.CaptureConflictRanges)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer, err := sink.New(ctx, cfg.ClickHouse, cfg.Ingest, matcher, cfg.FDB.ClusterName)
	if err != nil {
		return err
	}
	defer writer.Close()
	log.Info("connected to clickhouse", "address", cfg.ClickHouse.Address, "database", cfg.ClickHouse.Database)

	fdb.MustAPIVersion(cfg.FDB.APIVersion)
	db, err := fdb.OpenDatabase(cfg.FDB.ClusterFile)
	if err != nil {
		return fmt.Errorf("opening foundationdb (cluster file %q): %w", cfg.FDB.ClusterFile, err)
	}

	cursor, storedCaughtUp, err := writer.LoadCursor(ctx, ingesterID)
	if err != nil {
		return err
	}
	if cursor == nil {
		log.Info("no stored cursor; starting from the oldest record still retained", "ingester", ingesterID)
	} else {
		log.Info("resuming from stored cursor", "ingester", ingesterID,
			"cursor_bytes", len(cursor), "was_caught_up", storedCaughtUp)
	}

	d := &daemon{
		log: log,
		cfg: cfg,
		tailer: tail.New(db, tail.Options{
			Cursor:      cursor,
			MaxPartials: cfg.FDB.MaxPartials,
			MaxBytes:    cfg.FDB.MaxPollBytes,
		}),
		writer: writer,

		ingesterID:     ingesterID,
		storedCaughtUp: storedCaughtUp,
	}
	return d.loop(ctx, once)
}

type daemon struct {
	log        *slog.Logger
	cfg        config.Config
	tailer     *tail.Tailer
	writer     *sink.Writer
	ingesterID string

	pending []*profile.Record
	// pendingRows counts the rows the buffer will actually produce —
	// operations, mutations and conflict ranges — not just events. Mutations
	// dominate volume, so counting events alone would let one large commit
	// buffer unbounded memory under a "row" limit that barely moved.
	pendingRows  int
	lastFlush    time.Time
	recordsTotal uint64

	ringBytes      int64
	ringBytesValid bool

	// prof is the last profiling setting read from the cluster, and the rate
	// stamped on every row written. Kept across polls so a transient read
	// failure reuses the last known value rather than marking rows unknown.
	prof      fdbconfig.Profiling
	profValid bool
	// lastProf is the setting as last written to the series, so a change can
	// be recorded the moment it happens instead of at the next tick.
	lastProf      fdbconfig.Profiling
	profWritten   bool
	lastProfWrite time.Time

	// Loss bookkeeping. The pipeline sheds history rather than pressuring the
	// cluster, and the contract that makes the shedding acceptable is that
	// every shed is visible: WARN in the log, and a row in the series the
	// dashboard annotates. pending* accumulate events not yet written to
	// ClickHouse, so a failed sample write defers the evidence, never drops it.
	storedCaughtUp    bool
	lastCaughtUp      bool
	pendingLaps       uint64
	pendingResumeGaps uint64
	writtenDropped    uint64 // Stats.RecordsDropped already reported to the series
}

func (d *daemon) loop(ctx context.Context, once bool) error {
	d.lastFlush = time.Now()

	for {
		res, err := d.tailer.Poll(d.cfg.FDB.BatchSize)
		if err != nil {
			// A transient FoundationDB error must not kill the daemon; the
			// cursor has not moved, so the next poll retries the same range.
			d.log.Warn("poll failed, retrying", "err", err)
			if !d.sleep(ctx, d.cfg.FDB.PollInterval) {
				break
			}
			continue
		}
		if res.RingBytesValid {
			d.ringBytes, d.ringBytesValid = res.RingBytes, true
		}
		if res.ProfilingValid {
			d.prof, d.profValid = res.Profiling, true
		}
		if res.Lapped {
			d.noteLap(res.Lap)
		}
		d.recordProfiling(ctx)

		d.pending = append(d.pending, res.Records...)
		d.pendingRows += d.rowsOf(res.Records)

		// Caught up means no KEYS were read — not "no records completed".
		// A full batch of chunk halves or undecodable stretches must keep
		// draining at full speed, and must not let -once exit early.
		caughtUp := res.KeysRead == 0
		d.lastCaughtUp = caughtUp

		if d.shouldFlush(caughtUp) {
			d.flush(ctx)
		}

		if once && caughtUp {
			break
		}
		if caughtUp {
			if !d.sleep(ctx, d.cfg.FDB.PollInterval) {
				break
			}
		} else if ctx.Err() != nil {
			break
		}
	}

	// One last attempt on shutdown; if it fails the cursor was not advanced,
	// so the batch is replayed on the next start rather than lost.
	d.flush(context.Background())
	return nil
}

func (d *daemon) rowsOf(records []*profile.Record) int {
	n := 0
	for _, rec := range records {
		n += len(rec.Events)
		for i := range rec.Events {
			n += len(rec.Events[i].Mutations)
			if d.cfg.Ingest.CaptureConflictRanges {
				n += len(rec.Events[i].ReadConflictRanges) + len(rec.Events[i].WriteConflictRanges)
			}
		}
	}
	return n
}

func (d *daemon) shouldFlush(caughtUp bool) bool {
	if len(d.pending) == 0 {
		return false
	}
	if caughtUp {
		return true
	}
	if d.pendingRows >= d.cfg.ClickHouse.FlushRows {
		return true
	}
	return time.Since(d.lastFlush) >= d.cfg.ClickHouse.FlushInterval
}

// noteLap surfaces a detected ring-buffer lap: a hole torn in the profile
// history. Losing history is an accepted trade — the pipeline never pushes
// back on the cluster — but a silent hole is a bug, so this both warns and
// queues the event for the profiling_config series the dashboard annotates.
func (d *daemon) noteLap(lap *tail.Lap) {
	args := []any{
		"from_version", lap.FromVersion,
		"to_version", lap.ToVersion, // 0 = the keyspace was empty past the hole
	}
	if d.ringBytesValid {
		args = append(args, "ring_buffer_bytes", d.ringBytes)
	}
	if d.profValid {
		args = append(args, "size_limit", d.prof.SizeLimit)
	}

	switch {
	case lap.OnResume && d.storedCaughtUp:
		// The daemon was caught up when it stopped, so the trimming happened
		// while nothing watched; whether the trimmed span held unread records
		// is unknowable, which is exactly why it must be marked.
		d.pendingResumeGaps++
		d.log.Warn("history gap across a restart: the profile keyspace no longer reaches back to the stored cursor; "+
			"records written while no daemon was running may have been trimmed unread", args...)
	case lap.OnResume:
		d.pendingResumeGaps++
		d.log.Warn("history gap across a restart: the daemon stopped while still behind and the ring buffer "+
			"trimmed past its cursor; unread records were lost", args...)
	default:
		d.pendingLaps++
		d.log.Warn("ring buffer lapped the cursor: profile records were trimmed before they were read; "+
			"the dashboard shows this window as a history gap", args...)
	}
}

// recordProfiling appends to the profiling_config series: on every change to
// the sample rate or size limit, and otherwise once per configured interval.
//
// The interval matters as much as the change: an unbroken line of samples is
// what tells a reader that the rate really was 1.0 for the whole window, as
// opposed to nobody having looked. Failures are logged and dropped — a
// missing observation is a gap in a diagnostic series, not a reason to stop
// ingesting profiles.
func (d *daemon) recordProfiling(ctx context.Context) {
	if !d.profValid {
		return
	}
	changed := !d.profWritten ||
		d.lastProf.Enabled != d.prof.Enabled ||
		d.lastProf.Rate != d.prof.Rate ||
		d.lastProf.SizeLimit != d.prof.SizeLimit
	// Loss is written the moment it is seen, like a setting change: an
	// annotation that appears fifteen seconds after the hole would put the
	// marker visibly to the right of the data it explains.
	droppedDelta := d.tailer.Stats().RecordsDropped - d.writtenDropped
	loss := d.pendingLaps > 0 || d.pendingResumeGaps > 0 || droppedDelta > 0
	if !changed && !loss && time.Since(d.lastProfWrite) < d.cfg.Ingest.ProfilingSampleInterval {
		return
	}

	// Detached like flushOnce: this must still record "profiling was on when I
	// stopped" during a shutdown whose context is already cancelled.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	sample := sink.ProfilingSample{
		SampledAt:      time.Now(),
		Profiling:      d.prof,
		RingBytes:      d.ringBytes,
		RingBytesValid: d.ringBytesValid,
		Laps:           d.pendingLaps,
		ResumeGaps:     d.pendingResumeGaps,
		RecordsDropped: droppedDelta,
	}
	// Set before the attempt so a persistent failure retries at the interval
	// rather than on every poll.
	d.lastProfWrite = time.Now()
	if err := d.writer.WriteProfilingConfig(wctx, d.ingesterID, sample); err != nil {
		// lastProf is deliberately not advanced, so a change that failed to
		// write is still pending and gets another attempt.
		d.log.Warn("recording profiling config failed", "err", err)
		return
	}
	if changed && d.profWritten {
		d.log.Info("client profiling setting changed",
			"enabled", d.prof.Enabled, "sample_rate", d.prof.StoredRate(), "size_limit", d.prof.SizeLimit)
	}
	d.lastProf, d.profWritten = d.prof, true
	// Only after a successful write: a failed one keeps the loss pending, so
	// the evidence is deferred to the next sample rather than dropped.
	d.pendingLaps, d.pendingResumeGaps = 0, 0
	d.writtenDropped += droppedDelta
}

// flush writes the buffer and only then advances the stored cursor — that
// order is the at-least-once guarantee. It retries transient failures with
// backoff instead of dying: the buffer is bounded because polling stops while
// a flush is failing. It gives up only when ctx is cancelled; the unsaved
// cursor then replays the batch on the next start, so nothing is lost.
func (d *daemon) flush(ctx context.Context) {
	if len(d.pending) == 0 {
		return
	}

	backoff := time.Second
	for attempt := 1; ; attempt++ {
		err := d.flushOnce(ctx)
		if err == nil {
			return
		}
		d.log.Warn("flush failed", "attempt", attempt, "err", err)
		if ctx.Err() != nil {
			d.log.Warn("giving up on flush; the batch replays from the stored cursor on next start",
				"records", len(d.pending))
			return
		}
		if !d.sleep(ctx, backoff) {
			continue // one more attempt after cancellation, then the branch above exits
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

func (d *daemon) flushOnce(ctx context.Context) error {
	// The write must be able to complete during shutdown, when the signal
	// context is already cancelled — detached, with its own deadline.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()

	counts, err := d.writer.Write(fctx, d.pending, d.prof)
	if err != nil {
		return fmt.Errorf("writing to clickhouse: %w", err)
	}
	d.recordsTotal += uint64(len(d.pending))

	if err := d.writer.SaveCursor(fctx, d.ingesterID, d.tailer.Cursor(), d.recordsTotal, d.lastCaughtUp); err != nil {
		return fmt.Errorf("saving cursor: %w", err)
	}

	st := d.tailer.Stats()
	args := []any{
		"records", len(d.pending),
		"operations", counts.Operations,
		"mutations", counts.Mutations,
		"conflict_ranges", counts.ConflictRanges,
		"skipped_system", counts.SkippedSystem,
		"records_total", d.recordsTotal,
		"decode_errors", st.DecodeErrors,
		"dropped", st.RecordsDropped,
	}
	if d.profValid {
		args = append(args, "sample_rate", d.prof.StoredRate())
	}
	if d.ringBytesValid {
		// The ring buffer is the profile keyspace's size-capped FIFO: when it
		// fills, FoundationDB trims oldest-first, past this daemon's cursor if
		// it has fallen behind — which is warned about loudly when detected,
		// not just tallied here.
		args = append(args, "ring_buffer_bytes", d.ringBytes)
	}
	if laps := st.Laps; laps > 0 {
		args = append(args, "laps_total", laps)
	}
	d.log.Info("flushed", args...)

	d.pending = nil
	d.pendingRows = 0
	d.lastFlush = time.Now()
	return nil
}

// sleep waits, reporting false if the context was cancelled meanwhile.
func (d *daemon) sleep(ctx context.Context, dur time.Duration) bool {
	t := time.NewTimer(dur)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
