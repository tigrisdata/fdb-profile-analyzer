package tail

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/pboros/fdb-profile-analyzer/internal/profile"
)

// newTestTailer builds a Tailer with no database; consume, Cursor and the
// partial bookkeeping are pure and that is what these tests exercise.
func newTestTailer(clock *time.Time) *Tailer {
	opts := Options{}
	opts.fill()
	return &Tailer{
		opts:     opts,
		partials: make(map[string]*partial),
		now:      func() time.Time { return *clock },
	}
}

func chunkKey(version uint64, txnID string, chunk, total int32) []byte {
	k := append([]byte(nil), profile.KeyPrefix...)
	k = binary.BigEndian.AppendUint64(k, version)
	k = binary.BigEndian.AppendUint16(k, 0)
	k = append(k, '/')
	k = append(k, txnID...)
	k = append(k, '/')
	k = binary.BigEndian.AppendUint32(k, uint32(chunk))
	k = binary.BigEndian.AppendUint32(k, uint32(total))
	k = append(k, '/')
	return k
}

// recordValue builds a minimal valid record: one get event.
func recordValue(key string) []byte {
	var b []byte
	le64 := func(v uint64) { b = binary.LittleEndian.AppendUint64(b, v) }
	le32 := func(v uint32) { b = binary.LittleEndian.AppendUint32(b, v) }
	le64(profile.ProtocolVersion73)
	le32(uint32(profile.EventGet)) // event type
	le64(math.Float64bits(1.7e9))  // start timestamp
	le32(0)                        // dc id (empty)
	b = append(b, 0)               // tenant absent
	le64(math.Float64bits(0.001))  // latency
	le32(4)                        // value size
	le32(uint32(len(key)))
	b = append(b, key...)
	return b
}

// feed pushes one key/value through the tailer the way Poll would.
func feed(t *testing.T, tl *Tailer, key, value []byte) *profile.Record {
	t.Helper()
	prev := tl.lastRead
	rec, err := tl.consume(key, value, prev)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	tl.lastRead = append([]byte(nil), key...)
	return rec
}

// The property every restart depends on: while a multi-chunk record is open,
// the persistable cursor must not pass its first chunk.
func TestCursorLagsBehindOpenPartial(t *testing.T) {
	clock := time.Now()
	tl := newTestTailer(&clock)

	complete := chunkKey(100, "aaaaaaaaaaaaaaaa", 1, 1)
	if rec := feed(t, tl, complete, recordValue("/k1")); rec == nil {
		t.Fatal("single-chunk record should complete immediately")
	}
	if string(tl.Cursor()) != string(complete) {
		t.Fatalf("cursor should sit at the completed record")
	}

	// Open a 2-chunk record: the raw position advances, the cursor must not.
	full := recordValue("/big")
	half := len(full) / 2
	c1 := chunkKey(101, "bbbbbbbbbbbbbbbb", 1, 2)
	if rec := feed(t, tl, c1, full[:half]); rec != nil {
		t.Fatal("first chunk must not complete a record")
	}
	if string(tl.Cursor()) != string(complete) {
		t.Fatalf("cursor advanced past an incomplete record's first chunk:\n got %q\nwant %q",
			tl.Cursor(), complete)
	}

	// An unrelated complete record between the chunks must not move it either.
	mid := chunkKey(102, "cccccccccccccccc", 1, 1)
	feed(t, tl, mid, recordValue("/k2"))
	if string(tl.Cursor()) != string(complete) {
		t.Fatal("cursor moved while a partial was still open")
	}

	// Closing the partial releases the cursor to the raw position.
	c2 := chunkKey(103, "bbbbbbbbbbbbbbbb", 2, 2)
	rec := feed(t, tl, c2, full[half:])
	if rec == nil {
		t.Fatal("final chunk should complete the record")
	}
	if len(rec.Events) != 1 || rec.Events[0].Key != "/big" {
		t.Fatalf("reassembled record decoded wrong: %+v", rec.Events)
	}
	if string(tl.Cursor()) != string(c2) {
		t.Fatal("cursor should catch up once the partial completes")
	}
}

// Resuming from the safe cursor re-reads the partial's chunks and delivers the
// record — the at-least-once property across a restart.
func TestRestartFromCursorRedeliversPartial(t *testing.T) {
	clock := time.Now()
	tl := newTestTailer(&clock)

	anchor := chunkKey(100, "aaaaaaaaaaaaaaaa", 1, 1)
	feed(t, tl, anchor, recordValue("/k1"))

	full := recordValue("/big")
	feed(t, tl, chunkKey(101, "bbbbbbbbbbbbbbbb", 1, 2), full[:10])
	cursor := tl.Cursor()

	// "Restart": a fresh tailer resuming strictly after the cursor sees the
	// partial's chunks again, in order, and completes the record.
	tl2 := newTestTailer(&clock)
	tl2.lastRead = append([]byte(nil), cursor...)
	if rec := feed(t, tl2, chunkKey(101, "bbbbbbbbbbbbbbbb", 1, 2), full[:10]); rec != nil {
		t.Fatal("chunk 1 alone completed a record")
	}
	rec := feed(t, tl2, chunkKey(103, "bbbbbbbbbbbbbbbb", 2, 2), full[10:])
	if rec == nil {
		t.Fatal("record was not redelivered after resuming from the safe cursor")
	}
	if tl2.Stats().RecordsDropped != 0 {
		t.Fatalf("redelivery dropped records: %+v", tl2.Stats())
	}
}

func TestOrphanTailIsDroppedNotError(t *testing.T) {
	clock := time.Now()
	tl := newTestTailer(&clock)

	// Chunk 2 arrives with no chunk 1 — the head was trimmed away.
	rec := feed(t, tl, chunkKey(101, "bbbbbbbbbbbbbbbb", 2, 3), []byte("x"))
	if rec != nil {
		t.Fatal("orphan tail produced a record")
	}
	st := tl.Stats()
	if st.RecordsDropped != 1 || st.DecodeErrors != 0 {
		t.Fatalf("want 1 drop, 0 decode errors; got %+v", st)
	}
}

func TestChunkGapCountsOneDrop(t *testing.T) {
	clock := time.Now()
	tl := newTestTailer(&clock)

	feed(t, tl, chunkKey(101, "bbbbbbbbbbbbbbbb", 1, 3), []byte("x"))
	// Chunk 3 arrives next: chunk 2 was trimmed. One drop, not a decode error.
	rec := feed(t, tl, chunkKey(102, "bbbbbbbbbbbbbbbb", 3, 3), []byte("z"))
	if rec != nil {
		t.Fatal("gapped record produced a record")
	}
	st := tl.Stats()
	if st.RecordsDropped != 1 || st.DecodeErrors != 0 {
		t.Fatalf("want 1 drop, 0 decode errors; got %+v", st)
	}
	if len(tl.partials) != 0 {
		t.Fatal("broken partial should be discarded")
	}
}

func TestPartialExpiryReleasesCursor(t *testing.T) {
	clock := time.Now()
	tl := newTestTailer(&clock)

	anchor := chunkKey(100, "aaaaaaaaaaaaaaaa", 1, 1)
	feed(t, tl, anchor, recordValue("/k1"))
	feed(t, tl, chunkKey(101, "bbbbbbbbbbbbbbbb", 1, 2), []byte("x"))

	if string(tl.Cursor()) != string(anchor) {
		t.Fatal("cursor should be held back by the open partial")
	}

	clock = clock.Add(tl.opts.MaxPartialAge + time.Second)
	tl.expirePartials()

	st := tl.Stats()
	if st.PartialsExpired != 1 {
		t.Fatalf("want 1 expiry, got %+v", st)
	}
	if string(tl.Cursor()) == string(anchor) {
		t.Fatal("expiring the dead partial should release the cursor")
	}
}

func TestEvictionPrefersOldest(t *testing.T) {
	clock := time.Now()
	tl := newTestTailer(&clock)
	tl.opts.MaxPartials = 2

	feed(t, tl, chunkKey(101, "old_old_old_old_", 1, 2), []byte("a"))
	clock = clock.Add(time.Second)
	feed(t, tl, chunkKey(102, "new_new_new_new_", 1, 2), []byte("b"))
	clock = clock.Add(time.Second)
	// Cap reached: opening a third must evict the oldest, not a random one.
	feed(t, tl, chunkKey(103, "newest_newest_16", 1, 2), []byte("c"))

	if _, ok := tl.partials["old_old_old_old_"]; ok {
		t.Fatal("oldest partial survived eviction")
	}
	if _, ok := tl.partials["new_new_new_new_"]; !ok {
		t.Fatal("newer partial was evicted instead of the oldest")
	}
}

func TestLapCheck(t *testing.T) {
	k := func(version uint64) []byte { return chunkKey(version, "0123456789abcdef", 1, 1) }

	cases := []struct {
		name           string
		cursor, oldest []byte
		armed          bool
		want           bool
	}{
		// No cursor means "start from the oldest available" — there is nothing
		// to have lost, whatever the keyspace looks like.
		{"fresh start, empty keyspace", nil, nil, false, false},
		{"fresh start, data present", nil, k(100), false, false},

		// Disarmed = the previous poll exhausted the range, so the horizon
		// passing the cursor is the buffer's normal trimming of read data.
		{"caught up, horizon behind cursor", k(200), k(100), false, false},
		{"caught up, horizon passed cursor", k(200), k(300), false, false},

		// Armed = resuming, or the previous poll stopped on budget: keys were
		// believed to exist beyond the cursor.
		{"armed, horizon behind cursor", k(200), k(100), true, false},
		{"armed, horizon at cursor", k(200), k(200), true, false},
		{"armed, horizon passed cursor", k(200), k(300), true, true},
		{"armed, keyspace emptied", k(200), nil, true, true},
	}
	for _, tc := range cases {
		if got := lapCheck(tc.cursor, tc.oldest, tc.armed); got != tc.want {
			t.Errorf("%s: lapCheck = %v, want %v", tc.name, got, tc.want)
		}
	}
}
