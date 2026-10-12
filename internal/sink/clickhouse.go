// Package sink writes decoded profile records to ClickHouse.
package sink

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/pboros/fdb-profile-analyzer/internal/config"
	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
	"github.com/pboros/fdb-profile-analyzer/internal/prefix"
	"github.com/pboros/fdb-profile-analyzer/internal/profile"
)

// Open dials ClickHouse with the project's connection conventions. Exported so
// the test-bed tooling and the integration harness dial the same way the
// daemon does, instead of each restating the options.
func Open(ctx context.Context, cfg config.ClickHouse) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Address},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: cfg.DialTimeout,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		Settings: clickhouse.Settings{
			// Every table is written through its Distributed front, and a
			// Distributed insert acks by default once the *initiator* has
			// buffered the rows — before any shard has them. The daemon saves
			// its cursor after a write returns, so an async ack would let a
			// ClickHouse crash lose a batch the cursor has already moved past:
			// exactly the silent loss this pipeline exists to never commit.
			// Synchronous distributed inserts restore write-then-cursor; on
			// the single-shard topology this is a no-op.
			"insert_distributed_sync": 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("pinging clickhouse at %s: %w", cfg.Address, err)
	}
	return conn, nil
}

// Counts reports what a Write produced.
type Counts struct {
	Operations     int
	Mutations      int
	ConflictRanges int
	SkippedSystem  int
}

func (c *Counts) add(o Counts) {
	c.Operations += o.Operations
	c.Mutations += o.Mutations
	c.ConflictRanges += o.ConflictRanges
	c.SkippedSystem += o.SkippedSystem
}

// Writer inserts decoded records into ClickHouse.
type Writer struct {
	conn    driver.Conn
	db      string
	matcher *prefix.Matcher
	ingest  config.Ingest
	// cluster labels every row this writer produces with the FoundationDB
	// cluster the data came from (fdb.cluster_name), and scopes the stored
	// cursor. In production several clusters feed one ClickHouse; unlabelled
	// rows would be indistinguishable noise there.
	cluster string

	// Scratch space reused across records so classification allocates nothing
	// in steady state.
	evPfx  []string
	mutPfx []string
	mutOff []int
	counts map[string]int
}

// New connects to ClickHouse and returns a Writer that labels everything it
// writes as coming from the named FoundationDB cluster.
func New(ctx context.Context, cfg config.ClickHouse, ingest config.Ingest, m *prefix.Matcher, cluster string) (*Writer, error) {
	conn, err := Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Writer{
		conn:    conn,
		db:      cfg.Database,
		matcher: m,
		ingest:  ingest,
		cluster: cluster,
		counts:  make(map[string]int),
	}, nil
}

// Close releases the connection.
func (w *Writer) Close() error { return w.conn.Close() }

// eventTime converts a profile timestamp, which is Unix epoch seconds as a
// float, into a wall-clock time.
func eventTime(ts float64) time.Time {
	if ts <= 0 || math.IsNaN(ts) || math.IsInf(ts, 0) {
		return time.Unix(0, 0).UTC()
	}
	sec, frac := math.Modf(ts)
	return time.Unix(int64(sec), int64(frac*float64(time.Second))).UTC()
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// classRank orders prefix classes for the record-level vote: a configured
// prefix beats an (unknown) key label, which beats the informationless
// catch-alls, and (other) beats (system) so that a mixed record is kept
// rather than dropped.
func classRank(p string) int {
	switch {
	case p == prefix.System:
		return 0
	case p == prefix.Other:
		return 1
	case strings.HasSuffix(p, prefix.UnknownSuffix):
		return 2
	default:
		return 3
	}
}

// classify computes every prefix decision for a record in one pass: the prefix
// of each event, of each mutation, and the transaction-level winner that events
// without a key of their own (get_version, commit) inherit.
//
// The vote is deterministic: class rank first, then count, then the
// lexicographically smaller prefix — never map iteration order, which would
// let equal counts flip attribution (or, worse, the skip-system decision)
// between runs.
func (w *Writer) classify(rec *profile.Record) (recPrefix string) {
	w.evPfx = w.evPfx[:0]
	w.mutPfx = w.mutPfx[:0]
	w.mutOff = w.mutOff[:0]
	clear(w.counts)

	for i := range rec.Events {
		ev := &rec.Events[i]

		p := ""
		if ev.Key != "" {
			p = w.matcher.Match(ev.Key)
			w.counts[p]++
		}
		w.evPfx = append(w.evPfx, p)

		w.mutOff = append(w.mutOff, len(w.mutPfx))
		for j := range ev.Mutations {
			mp := w.matcher.Match(ev.Mutations[j].Param1)
			w.counts[mp]++
			w.mutPfx = append(w.mutPfx, mp)
		}
	}

	best, bestN := "", 0
	for p, n := range w.counts {
		if best == "" {
			best, bestN = p, n
			continue
		}
		switch br, pr := classRank(best), classRank(p); {
		case pr > br,
			pr == br && n > bestN,
			pr == br && n == bestN && p < best:
			best, bestN = p, n
		}
	}
	if best == "" {
		return prefix.Other
	}
	return best
}

// Write inserts every row implied by records. Batches are per-table; ClickHouse
// has no cross-table transaction, so a failure can leave one table ahead. The
// caller advances the cursor only after Write returns cleanly, so the effect of
// a failure is a replayed batch — which the ReplacingMergeTree sort keys
// collapse — never a lost one.
//
// prof is the cluster's profiling setting as the caller last observed it; its
// sample rate is stamped on every row so counts can be extrapolated back to
// cluster-wide totals later. Pass the zero value to record "unknown", which
// leaves those rows out of the extrapolation rather than scaling them wrongly.
func (w *Writer) Write(ctx context.Context, records []*profile.Record, prof fdbconfig.Profiling) (Counts, error) {
	var counts Counts
	if len(records) == 0 {
		return counts, nil
	}
	sampleRate := prof.StoredRate()

	// Every prepared batch pins a pooled connection until Send or Abort, so an
	// error path that abandons them leaks the pool empty. Track and abort
	// whatever was not sent.
	var open []driver.Batch
	defer func() {
		for _, b := range open {
			_ = b.Abort()
		}
	}()
	prepare := func(q string) (driver.Batch, error) {
		b, err := w.conn.PrepareBatch(ctx, q)
		if err != nil {
			return nil, err
		}
		open = append(open, b)
		return b, nil
	}
	send := func(b driver.Batch) error {
		for i, o := range open {
			if o == b {
				open = append(open[:i], open[i+1:]...)
				break
			}
		}
		// An empty batch still holds its connection; Abort releases it without
		// the round trip a Send would spend.
		if b.Rows() == 0 {
			return b.Abort()
		}
		return b.Send()
	}

	opsBatch, err := prepare(`INSERT INTO ` + w.db + `.operations (
		event_time, cluster, op_type, key_prefix, transaction_id, event_seq, versionstamp,
		dc_id, tenant, debug_id, latency, priority, read_version,
		value_size, range_size, key, end_key,
		num_mutations, commit_bytes, commit_version, read_snapshot_version,
		report_conflicting_keys, lock_aware, error_code, span_trace_id, span_id,
		sample_rate)`)
	if err != nil {
		return counts, fmt.Errorf("preparing operations batch: %w", err)
	}
	mutBatch, err := prepare(`INSERT INTO ` + w.db + `.mutations (
		event_time, cluster, key_prefix, mutation_type, transaction_id, event_seq,
		mutation_seq, key, value, value_size, sample_rate)`)
	if err != nil {
		return counts, fmt.Errorf("preparing mutations batch: %w", err)
	}
	var crBatch driver.Batch
	if w.ingest.CaptureConflictRanges {
		if crBatch, err = prepare(`INSERT INTO ` + w.db + `.conflict_ranges (
			event_time, cluster, key_prefix, kind, transaction_id, event_seq, range_seq,
			begin_key, end_key, sample_rate)`); err != nil {
			return counts, fmt.Errorf("preparing conflict_ranges batch: %w", err)
		}
	}

	for _, rec := range records {
		recPrefix := w.classify(rec)

		// The profiling machinery profiles its own maintenance transactions.
		// Left in, FoundationDB's bookkeeping tops every hotspot ranking on an
		// otherwise idle cluster, so system records are dropped whole.
		if w.ingest.SkipSystemKeyspace && recPrefix == prefix.System {
			counts.SkippedSystem++
			continue
		}

		c, err := w.appendRecord(opsBatch, mutBatch, crBatch, rec, recPrefix, sampleRate)
		if err != nil {
			return counts, err
		}
		counts.add(c)
	}

	if err := send(opsBatch); err != nil {
		return counts, fmt.Errorf("sending operations: %w", err)
	}
	if err := send(mutBatch); err != nil {
		return counts, fmt.Errorf("sending mutations: %w", err)
	}
	if crBatch != nil {
		if err := send(crBatch); err != nil {
			return counts, fmt.Errorf("sending conflict ranges: %w", err)
		}
	}
	return counts, nil
}

func (w *Writer) appendRecord(ops, muts, crs driver.Batch, rec *profile.Record, recPrefix string, sampleRate float64) (Counts, error) {
	var counts Counts

	for seq := range rec.Events {
		ev := &rec.Events[seq]
		ts := eventTime(ev.StartTimestamp)

		// An event with a key of its own is attributed to that key; one
		// without (get_version, commit) inherits the transaction's prefix.
		keyPrefix := w.evPfx[seq]
		if keyPrefix == "" {
			keyPrefix = recPrefix
		}

		var traceID string
		var spanID uint64
		if ev.Span != nil {
			var buf [16]byte
			binary.BigEndian.PutUint64(buf[:8], ev.Span.TraceIDHigh)
			binary.BigEndian.PutUint64(buf[8:], ev.Span.TraceIDLow)
			traceID = hex.EncodeToString(buf[:])
			spanID = ev.Span.SpanID
		}

		if err := ops.Append(
			ts, w.cluster, ev.Type.String(), keyPrefix, rec.Key.TxnID, uint32(seq), rec.Key.Version,
			ev.DCID, ev.Tenant, rec.Key.DebugID, ev.Latency, ev.Priority.String(), ev.ReadVersion,
			uint32(max(ev.ValueSize, 0)), uint32(max(ev.RangeSize, 0)), ev.Key, ev.EndKey,
			uint32(max(ev.NumMutations, 0)), uint32(max(ev.CommitBytes, 0)), ev.CommitVersion, ev.ReadSnapshotVersion,
			boolToUint8(ev.ReportConflictingKeys), boolToUint8(ev.LockAware), ev.ErrorCode, traceID, spanID,
			sampleRate,
		); err != nil {
			return counts, fmt.Errorf("appending operation: %w", err)
		}
		counts.Operations++

		mutPfx := w.mutPfx[w.mutOff[seq] : w.mutOff[seq]+len(ev.Mutations)]
		for i := range ev.Mutations {
			m := &ev.Mutations[i]
			value := m.Param2
			if !w.ingest.CaptureMutationValues {
				value = ""
			}
			if err := muts.Append(
				ts, w.cluster, mutPfx[i], m.Type.String(), rec.Key.TxnID, uint32(seq),
				uint32(i), m.Param1, value, uint32(len(m.Param2)), sampleRate,
			); err != nil {
				return counts, fmt.Errorf("appending mutation: %w", err)
			}
			counts.Mutations++
		}

		if crs != nil {
			for i := range ev.ReadConflictRanges {
				r := &ev.ReadConflictRanges[i]
				if err := crs.Append(ts, w.cluster, w.matcher.Match(r.Begin), "read",
					rec.Key.TxnID, uint32(seq), uint32(i), r.Begin, r.End, sampleRate); err != nil {
					return counts, fmt.Errorf("appending read conflict range: %w", err)
				}
				counts.ConflictRanges++
			}
			for i := range ev.WriteConflictRanges {
				r := &ev.WriteConflictRanges[i]
				if err := crs.Append(ts, w.cluster, w.matcher.Match(r.Begin), "write",
					rec.Key.TxnID, uint32(seq), uint32(i), r.Begin, r.End, sampleRate); err != nil {
					return counts, fmt.Errorf("appending write conflict range: %w", err)
				}
				counts.ConflictRanges++
			}
		}
	}
	return counts, nil
}

// LoadCursor returns the stored position for an ingester, or nil if it has
// never written one. caughtUp is whether the daemon that saved it believed it
// had drained the keyspace at the time — which decides how a gap found on
// resume is worded: a caught-up daemon's downtime gap *may* have lost records,
// a behind daemon's gap certainly did.
func (w *Writer) LoadCursor(ctx context.Context, ingesterID string) (cursor []byte, caughtUp bool, err error) {
	var cursorHex string
	var caught uint8
	row := w.conn.QueryRow(ctx,
		`SELECT cursor_hex, caught_up FROM `+w.db+`.ingest_cursor
		 WHERE cluster = ? AND ingester_id = ?
		 ORDER BY cursor_version DESC LIMIT 1`, w.cluster, ingesterID)
	if err := row.Scan(&cursorHex, &caught); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The normal first run.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("loading cursor: %w", err)
	}
	if cursorHex == "" {
		return nil, false, nil
	}
	b, err := hex.DecodeString(cursorHex)
	if err != nil {
		return nil, false, fmt.Errorf("decoding stored cursor %q: %w", cursorHex, err)
	}
	return b, caught != 0, nil
}

// SaveCursor records the position. Call it only after the rows it covers are
// in. The replace-merge version is the cursor key's own commit versionstamp —
// monotonic by construction — never the wall clock, which a backwards NTP step
// or a host swap could rewind, resurrecting a stale cursor.
// caughtUp records whether the tailer had drained the keyspace when this
// position was taken; see LoadCursor for what it decides.
func (w *Writer) SaveCursor(ctx context.Context, ingesterID string, cursor []byte, total uint64, caughtUp bool) error {
	if len(cursor) == 0 {
		return nil
	}
	key, err := profile.ParseKey(cursor)
	if err != nil {
		return fmt.Errorf("cursor is not a profile key: %w", err)
	}
	batch, err := w.conn.PrepareBatch(ctx,
		`INSERT INTO `+w.db+`.ingest_cursor (cluster, ingester_id, cursor_hex, cursor_version, records_total, caught_up, updated_at)`)
	if err != nil {
		return fmt.Errorf("preparing cursor batch: %w", err)
	}
	if err := batch.Append(w.cluster, ingesterID, hex.EncodeToString(cursor), key.Version, total, boolToUint8(caughtUp), time.Now().UTC()); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("appending cursor: %w", err)
	}
	return batch.Send()
}

// ProfilingSample is one observation of the cluster's client profiling
// settings, as seen by a tailing daemon at a moment in time.
type ProfilingSample struct {
	SampledAt time.Time
	Profiling fdbconfig.Profiling
	// RingBytes is how much of the size limit the profile keyspace is using.
	// Valid only when RingBytesValid; the counter is absent until something has
	// been profiled.
	RingBytes      int64
	RingBytesValid bool

	// Loss since the previous sample. Laps and ResumeGaps are holes torn in
	// the history by the ring buffer — live laps and restart-time gaps
	// respectively — each of unknowable size, so they are counted as events,
	// never as records. RecordsDropped is the countable structural loss:
	// records whose remaining chunks were trimmed or evicted mid-reassembly.
	// These ride on the config series because that is the dashboard's
	// authoritative timeline of the profiling machinery itself: the loss lands
	// at the moment it was seen, next to the settings that caused it.
	Laps           uint64
	ResumeGaps     uint64
	RecordsDropped uint64
}

// WriteProfilingConfig records one observation of the profiling settings.
//
// Written outside the record batches on purpose: the series has to keep
// ticking while nothing is being sampled at all, which is exactly when knowing
// the rate is zero matters most.
func (w *Writer) WriteProfilingConfig(ctx context.Context, ingesterID string, s ProfilingSample) error {
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO `+w.db+`.profiling_config (
		sampled_at, cluster, ingester_id, enabled, sample_rate, size_limit,
		ring_buffer_bytes, ring_buffer_valid, laps, resume_gaps, records_dropped)`)
	if err != nil {
		return fmt.Errorf("preparing profiling_config batch: %w", err)
	}
	if err := batch.Append(
		s.SampledAt.UTC(), w.cluster, ingesterID,
		boolToUint8(s.Profiling.Enabled), s.Profiling.StoredRate(),
		uint64(max(s.Profiling.SizeLimit, 0)),
		uint64(max(s.RingBytes, 0)), boolToUint8(s.RingBytesValid),
		s.Laps, s.ResumeGaps, s.RecordsDropped,
	); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("appending profiling config: %w", err)
	}
	return batch.Send()
}
