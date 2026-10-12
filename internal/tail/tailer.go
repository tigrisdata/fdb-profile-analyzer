// Package tail follows FoundationDB's client profile keyspace, reassembling
// chunked records and handing back decoded profiles.
package tail

import (
	"bytes"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
	"github.com/pboros/fdb-profile-analyzer/internal/profile"
)

// partial accumulates the chunks of a record that spans more than one key.
//
// Chunks of one record are not necessarily adjacent: the client splits a large
// batch across several commits, so chunks can carry different versionstamps
// with unrelated records interleaved between them. They are therefore buffered
// by transaction id rather than assumed contiguous.
type partial struct {
	numChunks int32
	nextChunk int32 // the chunk number expected next, 1-based
	data      []byte

	// resumeKey is the cursor value from just before this record's first
	// chunk. Resuming at it re-reads every chunk of this record, which is what
	// makes a restart at-least-once instead of silently losing the record.
	resumeKey []byte
	firstSeen time.Time
}

// Stats reports what the tailer has seen, for operational visibility.
type Stats struct {
	KeysRead        uint64
	RecordsComplete uint64
	// RecordsDropped counts records lost for structural reasons: orphaned
	// tails after ring-buffer trimming, chunk-sequence gaps, evictions and
	// expiries. Distinct from DecodeErrors, which are malformed bytes.
	RecordsDropped      uint64
	PartialsExpired     uint64
	DecodeErrors        uint64
	UnsupportedProtocol uint64
	// Laps counts detected ring-buffer laps: occasions where the trim horizon
	// passed the read position and unread records were deleted. Each is a hole
	// in the recorded history, of unknowable size.
	Laps uint64
}

// Options configures a Tailer.
type Options struct {
	// Cursor resumes from a previous run. Nil starts at the oldest record
	// still in the keyspace.
	Cursor []byte
	// MaxPartials bounds how many incomplete records are held at once.
	// Default 1000.
	MaxPartials int
	// MaxPartialAge is how long an incomplete record is held before being
	// declared dead — its remaining chunks were most likely trimmed away, or
	// the writing client crashed between commits. Default 5m.
	MaxPartialAge time.Duration
	// MaxBytes caps how much key+value data one Poll reads. Chunk values run
	// up to ~100KB, so a key-count limit alone lets a backlog poll attempt
	// far more than FoundationDB's 5-second read window can deliver.
	// Default 32MB.
	MaxBytes int
	// TransactionTimeout bounds each poll's FDB transaction, so a poll against
	// an unreachable cluster fails in bounded time instead of blocking
	// shutdown forever. Default 15s.
	TransactionTimeout time.Duration
}

func (o *Options) fill() {
	if o.MaxPartials <= 0 {
		o.MaxPartials = 1000
	}
	if o.MaxPartialAge <= 0 {
		o.MaxPartialAge = 5 * time.Minute
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = 32 << 20
	}
	if o.TransactionTimeout <= 0 {
		o.TransactionTimeout = 15 * time.Second
	}
}

// Tailer reads forward through the profile keyspace, remembering where it got
// to. Because the versionstamp leads the key, the keyspace is append-ordered
// and resuming is just "read everything after the cursor".
type Tailer struct {
	db   fdb.Database
	opts Options

	// tr is reused across polls with Reset. The bindings destroy a
	// transaction's C object only from a GC finalizer, so a
	// transaction-per-poll leaks C memory on an idle process whose Go heap
	// never triggers a collection.
	tr     fdb.Transaction
	haveTr bool

	// lastRead is the last key consumed, complete or not. The resumable
	// position is Cursor(), which may deliberately lag behind this.
	lastRead []byte

	// armed reports whether a lap check on the next poll is meaningful: true
	// while resuming from a stored cursor and after any poll that stopped
	// before exhausting the range. A caught-up tailer disarms itself, because
	// the trim horizon passing an already-read position is the ring buffer's
	// normal, lossless behaviour.
	armed bool
	// resuming marks the first poll after construction with a cursor, so a
	// lap there can be reported as a restart-time gap rather than a live lap.
	resuming bool

	partials map[string]*partial

	now   func() time.Time // test seam
	stats Stats
}

// New creates a Tailer over db.
func New(db fdb.Database, opts Options) *Tailer {
	opts.fill()
	return &Tailer{
		db:       db,
		opts:     opts,
		lastRead: append([]byte(nil), opts.Cursor...),
		// A stored cursor arms the first poll: the tailer cannot know whether
		// it was caught up when that cursor was saved, and the check is what
		// turns a lap during downtime into a visible gap instead of silence.
		armed:    len(opts.Cursor) > 0,
		resuming: len(opts.Cursor) > 0,
		partials: make(map[string]*partial),
		now:      time.Now,
	}
}

// Cursor returns the position to persist: the newest point from which a
// restart re-reads every chunk of every record not yet delivered. While a
// multi-chunk record is incomplete this deliberately lags the raw read
// position — persisting past buffered chunks would turn a restart into silent
// loss of exactly the largest records. The value is monotonic: a partial's
// resume key is captured from the read position, which only moves forward.
//
// Store it only after the records from the corresponding Poll have been
// durably written downstream.
func (t *Tailer) Cursor() []byte {
	cur := t.lastRead
	for _, p := range t.partials {
		// A nil resumeKey means the partial began at the very start of the
		// keyspace, so the safe position is "the beginning".
		if p.resumeKey == nil {
			return nil
		}
		if bytes.Compare(p.resumeKey, cur) < 0 {
			cur = p.resumeKey
		}
	}
	if cur == nil {
		return nil
	}
	return append([]byte(nil), cur...)
}

// Stats returns a snapshot of the counters.
func (t *Tailer) Stats() Stats { return t.stats }

// Lap describes one detected hole in the profile history: the ring buffer's
// trim horizon moved past the read position, so records in (From, To) were
// deleted before they were ever read.
type Lap struct {
	// OnResume distinguishes a gap discovered on the first poll after a
	// restart — where the loss happened while no tailer was running and the
	// exact extent is unknowable — from a live lap, where the tailer was
	// mid-drain and the trimmed keys are near-certain loss.
	OnResume bool
	// FromVersion is the commit version of the cursor the hole starts after;
	// ToVersion is the version of the first surviving record, or 0 when the
	// keyspace is empty. Versions rather than times: they are what the profile
	// keys carry, and what an operator can correlate with cluster logs.
	FromVersion uint64
	ToVersion   uint64
}

// lapCheck is the decision, kept pure so it can be tested without a cluster.
//
// cursor is the read position at the start of the poll; oldest is the first
// key currently in the profile keyspace (nil when the keyspace is empty);
// armed is the tailer's belief that unread keys may exist at or before the
// horizon — set while resuming or mid-drain, cleared once a poll exhausts the
// range. Profile keys begin with a commit versionstamp, so the keyspace is
// append-ordered and trimmed strictly from the front: when armed, a horizon
// past the cursor can only have got there by deleting keys that were never
// read.
//
// The one known false positive: a poll that stops at exactly its key budget
// with nothing left behind it stays armed, and if the trimmer then consumes
// everything up to that position before the next poll, the next oldest key is
// a fresh append and this reports a lap that lost nothing. That requires a
// budget-sized backlog to end on the exact final key and the whole buffer to
// turn over inside one poll interval — accepted, because the alternative
// (disarming on full polls) would silence the detector precisely while it is
// draining a backlog, which is when laps actually happen.
func lapCheck(cursor, oldest []byte, armed bool) bool {
	if !armed || len(cursor) == 0 {
		return false
	}
	if len(oldest) == 0 {
		// Armed with a cursor and the keyspace is empty: everything, read and
		// unread alike, has been trimmed.
		return true
	}
	return bytes.Compare(oldest, cursor) > 0
}

// PollResult is what one Poll saw.
type PollResult struct {
	Records []*profile.Record
	// KeysRead distinguishes "nothing new in the keyspace" (0) from "keys were
	// read but no record completed yet" (>0) — chunks of a still-open record,
	// orphaned tails, undecodable stretches. Callers deciding whether they
	// have caught up must use this, not len(Records).
	KeysRead  int
	BytesRead int
	// RingBytes is the profile keyspace's running size counter, read in the
	// same transaction: the headroom left before the size-capped ring buffer
	// starts trimming unread data. Valid only when RingBytesValid.
	RingBytes      int64
	RingBytesValid bool
	// Profiling is the cluster's client profiling setting, read in the same
	// transaction for the same reason: the sample rate is what every count
	// derived from these records has to be divided by to mean anything, and it
	// changes under the daemon's feet whenever an operator retunes it. Valid
	// only when ProfilingValid.
	Profiling      fdbconfig.Profiling
	ProfilingValid bool
	// Lap is set when this poll detected that the ring buffer trimmed records
	// that were never read. The pipeline sheds history rather than pressuring
	// the cluster, so this is not an error — but it must never be silent:
	// callers are expected to surface it all the way to the dashboard.
	Lap    *Lap
	Lapped bool
}

// Poll reads forward from the cursor and returns every record that became
// complete, stopping at the configured key and byte budgets. A result with
// KeysRead == 0 means the tailer has caught up.
func (t *Tailer) Poll(limit int) (PollResult, error) {
	var res PollResult
	t.expirePartials()

	if err := t.resetTransaction(); err != nil {
		return res, err
	}

	// Issued before the drain so they resolve alongside it instead of costing
	// their own round trip afterwards. Same snapshot, so the oldest key and the
	// counter describe the same moment as the range read.
	oldestFut := t.tr.Snapshot().GetKey(fdb.FirstGreaterOrEqual(fdb.Key(profile.KeyPrefix)))
	counterFut := t.tr.Snapshot().Get(fdb.Key(profile.CounterKey))
	cursorAtStart := append([]byte(nil), t.lastRead...)

	begin := fdb.KeySelector(fdb.FirstGreaterOrEqual(fdb.Key(profile.KeyPrefix)))
	if len(t.lastRead) > 0 {
		begin = fdb.FirstGreaterThan(fdb.Key(t.lastRead))
	}
	rng := fdb.SelectorRange{Begin: begin, End: fdb.FirstGreaterOrEqual(fdb.Key(profile.EndKey))}

	// Snapshot read: this is a read-only follower, and conflict ranges over a
	// keyspace every client writes to would be pointless contention. Iterator
	// mode streams in server-sized batches so progress is made even when the
	// full budget cannot be read inside FDB's 5-second window.
	it := t.tr.Snapshot().GetRange(rng, fdb.RangeOptions{
		Limit: limit,
		Mode:  fdb.StreamingModeIterator,
	}).Iterator()

	// Exhausting the range is what disarms the lap check: it is the one
	// moment the tailer positively knows nothing unread exists behind the
	// horizon. Stopping on budget — bytes, keys, or a mid-drain error — leaves
	// it armed.
	exhausted := true
	for it.Advance() {
		kv, err := it.Get()
		if err != nil {
			if res.KeysRead > 0 {
				// The budget outran the transaction (typically 1007,
				// transaction_too_old). Everything consumed so far is real
				// progress; the next poll continues from the new cursor.
				t.armed = true
				return res, nil
			}
			return res, fmt.Errorf("reading profile keyspace: %w", err)
		}

		prev := t.lastRead
		rec, err := t.consume(kv.Key, kv.Value, prev)
		t.lastRead = append(t.lastRead[:0], kv.Key...)
		res.KeysRead++
		res.BytesRead += len(kv.Key) + len(kv.Value)
		t.stats.KeysRead++

		if err != nil {
			if profile.IsUnsupportedProtocol(err) {
				t.stats.UnsupportedProtocol++
			} else {
				t.stats.DecodeErrors++
			}
		} else if rec != nil {
			t.stats.RecordsComplete++
			res.Records = append(res.Records, rec)
		}

		if res.BytesRead >= t.opts.MaxBytes {
			exhausted = false
			break
		}
	}
	// A drain that ends on exactly the key limit is ambiguous — the range may
	// or may not have ended there — and counts as not exhausted; see lapCheck
	// for why the ambiguity resolves toward staying armed.
	if res.KeysRead >= limit {
		exhausted = false
	}

	// The lap check compares this snapshot's oldest surviving key against the
	// position this poll started from. Best effort like the reads below it:
	// a poll that spent its transaction budget skips the check rather than
	// failing, and the next poll repeats it anyway.
	if oldestRaw, err := oldestFut.Get(); err == nil {
		var oldest []byte
		if bytes.Compare(oldestRaw, profile.KeyPrefix) >= 0 && bytes.Compare(oldestRaw, profile.EndKey) < 0 {
			oldest = oldestRaw
		}
		if lapCheck(cursorAtStart, oldest, t.armed) {
			lap := &Lap{OnResume: t.resuming}
			if k, err := profile.ParseKey(cursorAtStart); err == nil {
				lap.FromVersion = k.Version
			}
			if k, err := profile.ParseKey(oldest); oldest != nil && err == nil {
				lap.ToVersion = k.Version
			}
			res.Lap, res.Lapped = lap, true
			t.stats.Laps++
		}
	}
	t.resuming = false
	t.armed = !exhausted

	// The ring-buffer counter rides along in the same transaction, so tracking
	// headroom costs no extra transaction. Best effort: if the range read
	// spent the transaction's budget, skip it rather than fail the poll.
	if cv, err := counterFut.Get(); err == nil && len(cv) == 8 {
		res.RingBytes = int64(uint64(cv[0]) | uint64(cv[1])<<8 | uint64(cv[2])<<16 | uint64(cv[3])<<24 |
			uint64(cv[4])<<32 | uint64(cv[5])<<40 | uint64(cv[6])<<48 | uint64(cv[7])<<56)
		res.RingBytesValid = true
	}

	// Likewise the sample rate and size limit. Best effort for the same
	// reason: a poll that produced records must not fail because the
	// transaction had no budget left for two extra point reads.
	if p, err := fdbconfig.ReadFrom(t.tr.Snapshot()); err == nil {
		res.Profiling, res.ProfilingValid = p, true
	}
	return res, nil
}

func (t *Tailer) resetTransaction() error {
	if !t.haveTr {
		tr, err := t.db.CreateTransaction()
		if err != nil {
			return err
		}
		t.tr = tr
		t.haveTr = true
	} else {
		t.tr.Reset()
	}
	// Reset clears options, so they are applied every poll. The profile
	// keyspace lives under \xff\x02, hence system-key access; lock-aware so a
	// locked cluster still drains; the timeout bounds how long a poll can
	// block against an unreachable cluster.
	if err := t.tr.Options().SetReadSystemKeys(); err != nil {
		return err
	}
	if err := t.tr.Options().SetReadLockAware(); err != nil {
		return err
	}
	return t.tr.Options().SetTimeout(t.opts.TransactionTimeout.Milliseconds())
}

// consume folds one key/value into the tailer, returning a record once the last
// chunk of one arrives. prevCursor is the read position from before this key,
// captured as the resume point when this key opens a new partial.
func (t *Tailer) consume(rawKey, value, prevCursor []byte) (*profile.Record, error) {
	key, err := profile.ParseKey(rawKey)
	if err != nil {
		return nil, err
	}

	if key.Single() {
		rec, err := profile.DecodeValue(value)
		if err != nil {
			return nil, err
		}
		rec.Key = key
		return rec, nil
	}

	p, ok := t.partials[key.TxnID]
	if !ok {
		if key.ChunkNum != 1 {
			// The head of this record is already gone — most likely trimmed
			// by the ring buffer, or dropped by an earlier eviction. Nothing
			// to reassemble; count it once, as a drop, not a decode error.
			t.stats.RecordsDropped++
			return nil, nil
		}
		t.evictIfFull()
		p = &partial{
			numChunks: key.NumChunks,
			nextChunk: 1,
			// The first chunk's size bounds the rest, so this is an accurate
			// allocation for the whole record.
			data:      make([]byte, 0, int(key.NumChunks)*len(value)),
			firstSeen: t.now(),
		}
		if prevCursor != nil {
			p.resumeKey = append([]byte(nil), prevCursor...)
		}
		t.partials[key.TxnID] = p
	}

	if key.ChunkNum != p.nextChunk || key.NumChunks != p.numChunks {
		// A gap in the sequence: a middle chunk was trimmed away. The record
		// is unusable; one drop, no decode error.
		delete(t.partials, key.TxnID)
		t.stats.RecordsDropped++
		return nil, nil
	}

	p.data = append(p.data, value...)
	p.nextChunk++

	if key.ChunkNum != key.NumChunks {
		return nil, nil
	}

	delete(t.partials, key.TxnID)
	rec, err := profile.DecodeValue(p.data)
	if err != nil {
		return nil, err
	}
	rec.Key = key
	return rec, nil
}

// expirePartials drops incomplete records whose remaining chunks are overdue.
// Without this, a record whose tail was trimmed away (or whose writing client
// died between commits) would pin the safe cursor forever.
func (t *Tailer) expirePartials() {
	if len(t.partials) == 0 {
		return
	}
	cutoff := t.now().Add(-t.opts.MaxPartialAge)
	for id, p := range t.partials {
		if p.firstSeen.Before(cutoff) {
			delete(t.partials, id)
			t.stats.PartialsExpired++
			t.stats.RecordsDropped++
		}
	}
}

// evictIfFull makes room by dropping the OLDEST partial — the one holding the
// safe cursor furthest back. Evicting the oldest both frees the most cursor
// progress and is the fairest guess about which record will never complete.
func (t *Tailer) evictIfFull() {
	if len(t.partials) < t.opts.MaxPartials {
		return
	}
	var victim string
	var oldest time.Time
	for id, p := range t.partials {
		if victim == "" || p.firstSeen.Before(oldest) {
			victim, oldest = id, p.firstSeen
		}
	}
	if victim != "" {
		delete(t.partials, victim)
		t.stats.RecordsDropped++
	}
}
