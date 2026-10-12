package profile

import (
	"encoding/binary"
	"math"
	"testing"
)

// builder writes the on-disk profile format, so tests can construct records
// the way FoundationDB would.
type builder struct{ b []byte }

func (w *builder) u8(v uint8) { w.b = append(w.b, v) }
func (w *builder) bool(v bool) {
	if v {
		w.u8(1)
	} else {
		w.u8(0)
	}
}
func (w *builder) u64(v uint64) {
	var t [8]byte
	binary.LittleEndian.PutUint64(t[:], v)
	w.b = append(w.b, t[:]...)
}
func (w *builder) i64(v int64) { w.u64(uint64(v)) }
func (w *builder) i32(v int32) {
	var t [4]byte
	binary.LittleEndian.PutUint32(t[:], uint32(v))
	w.b = append(w.b, t[:]...)
}
func (w *builder) f64(v float64) { w.u64(math.Float64bits(v)) }
func (w *builder) str(s string)  { w.i32(int32(len(s))); w.b = append(w.b, s...) }

// header writes the fields every event shares, for a 7.3 record.
func (w *builder) header(typ EventType, ts float64, dc string) {
	w.i32(int32(typ))
	w.f64(ts)
	w.str(dc)     // dc id, present since 6.3
	w.bool(false) // legacy tenant Optional, absent
}

func (w *builder) get(ts float64, latency float64, valueSize int32, key string) {
	w.header(EventGet, ts, "dc1")
	w.f64(latency)
	w.i32(valueSize)
	w.str(key)
}

// commit writes a commit event. withSpan controls the trailing
// Optional<SpanContext>, which is the field most likely to be mis-sized.
func (w *builder) commit(ts float64, muts []Mutation, withSpan bool) {
	w.header(EventCommit, ts, "dc1")
	w.f64(0.004)            // latency
	w.i32(int32(len(muts))) // num mutations
	w.i32(1234)             // commit bytes
	w.i64(99)               // commit version
	w.i32(0)                // read conflict ranges
	w.i32(1)                // write conflict ranges
	w.str("wa")
	w.str("wb")
	w.i32(int32(len(muts)))
	for _, m := range muts {
		w.u8(uint8(m.Type))
		w.str(m.Param1)
		w.str(m.Param2)
	}
	w.i64(88)        // read snapshot version
	w.bool(false)    // report conflicting keys
	w.bool(false)    // lock aware
	w.bool(withSpan) // span presence
	if withSpan {
		w.u64(0x1111111111111111) // trace id high
		w.u64(0x2222222222222222) // trace id low
		w.u64(0x3333333333333333) // span id   — 7.2+ only
		w.u8(1)                   // flags     — 7.2+ only
	}
}

func record(fn func(*builder)) []byte {
	var w builder
	w.u64(ProtocolVersion73)
	fn(&w)
	return w.b
}

func TestDecodeGet(t *testing.T) {
	raw := record(func(w *builder) { w.get(1700000000.5, 0.002, 42, "/users/k1") })

	rec, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if len(rec.Events) != 1 {
		t.Fatalf("got %d events, want 1", len(rec.Events))
	}
	ev := rec.Events[0]
	if ev.Type != EventGet {
		t.Errorf("Type = %v, want get", ev.Type)
	}
	if ev.StartTimestamp != 1700000000.5 {
		t.Errorf("StartTimestamp = %v", ev.StartTimestamp)
	}
	if ev.Latency != 0.002 || ev.ValueSize != 42 || ev.Key != "/users/k1" {
		t.Errorf("unexpected payload: %+v", ev)
	}
	if ev.DCID != "dc1" {
		t.Errorf("DCID = %q", ev.DCID)
	}
}

func TestDecodeCommitMutations(t *testing.T) {
	muts := []Mutation{
		{Type: MutationSetValue, Param1: "/users/a", Param2: "v1"},
		{Type: MutationClearRange, Param1: "/users/b", Param2: "/users/c"},
	}
	raw := record(func(w *builder) { w.commit(1700000001, muts, false) })

	rec, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	ev := rec.Events[0]
	if ev.Type != EventCommit {
		t.Fatalf("Type = %v, want commit", ev.Type)
	}
	if len(ev.Mutations) != 2 {
		t.Fatalf("got %d mutations, want 2", len(ev.Mutations))
	}
	if ev.Mutations[0] != muts[0] || ev.Mutations[1] != muts[1] {
		t.Errorf("mutations round-tripped wrong: %+v", ev.Mutations)
	}
	// Values are captured in full, per the "record everything" decision.
	if ev.Mutations[0].Param2 != "v1" {
		t.Errorf("mutation value dropped: %+v", ev.Mutations[0])
	}
	if len(ev.WriteConflictRanges) != 1 || ev.WriteConflictRanges[0].Begin != "wa" {
		t.Errorf("write conflict ranges: %+v", ev.WriteConflictRanges)
	}
	if ev.CommitVersion != 99 || ev.ReadSnapshotVersion != 88 {
		t.Errorf("versions: commit=%d snapshot=%d", ev.CommitVersion, ev.ReadSnapshotVersion)
	}
}

// A commit carrying a span context is followed by another event. On 7.2+ the
// span is 25 bytes; reading it as 16 leaves 9 bytes unconsumed and every later
// event in the record decodes to garbage. That is the live bug in
// FoundationDB's transaction_profiling_analyzer.py, so it gets its own test.
func TestSpanContextDoesNotDesyncFollowingEvents(t *testing.T) {
	raw := record(func(w *builder) {
		w.commit(1700000002, []Mutation{{Type: MutationSetValue, Param1: "/k", Param2: "v"}}, true)
		w.get(1700000003, 0.001, 7, "/users/after-span")
	})

	rec, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if len(rec.Events) != 2 {
		t.Fatalf("got %d events, want 2 — the span was mis-sized", len(rec.Events))
	}

	span := rec.Events[0].Span
	if span == nil {
		t.Fatal("span not decoded")
	}
	if span.TraceIDHigh != 0x1111111111111111 || span.TraceIDLow != 0x2222222222222222 {
		t.Errorf("trace id = %#x%#x", span.TraceIDHigh, span.TraceIDLow)
	}
	if span.SpanID != 0x3333333333333333 || span.Flags != 1 {
		t.Errorf("span id = %#x flags = %d", span.SpanID, span.Flags)
	}

	after := rec.Events[1]
	if after.Type != EventGet || after.Key != "/users/after-span" || after.ValueSize != 7 {
		t.Errorf("event after the span decoded wrong: %+v", after)
	}
}

func TestDecodeRejectsOldProtocol(t *testing.T) {
	var w builder
	w.u64(ProtocolVersion62)
	if _, err := DecodeValue(w.b); err == nil {
		t.Fatal("want an error for a pre-6.3 record")
	} else if !IsUnsupportedProtocol(err) {
		t.Fatalf("want ErrUnsupportedProtocol, got %v", err)
	}
}

func TestDecodeRejectsUnknownMutationType(t *testing.T) {
	raw := record(func(w *builder) {
		w.header(EventCommit, 1700000004, "dc1")
		w.f64(0.001)
		w.i32(1)
		w.i32(10)
		w.i64(1)
		w.i32(0)
		w.i32(0)
		w.i32(1)
		w.u8(200) // not a valid MutationRef::Type
		w.str("k")
		w.str("v")
	})
	if _, err := DecodeValue(raw); err == nil {
		t.Fatal("want an error for an out-of-range mutation type")
	}
}

func TestDecodeTruncatedRecord(t *testing.T) {
	raw := record(func(w *builder) { w.get(1700000005, 0.001, 1, "/users/k") })
	for n := 9; n < len(raw); n++ {
		if _, err := DecodeValue(raw[:n]); err == nil {
			t.Fatalf("truncating to %d bytes should have failed", n)
		}
	}
}

// FoundationDB writes tr.Clear(key) — a ClearRange whose end is keyAfter(key) —
// as (type, keyAfter(key), ""). The decoder must reconstruct the original pair
// exactly as FDB's deserializer does; upstream's Python analyzer does not, and
// this test pins the difference.
func TestDecodeSingleKeyClearIsReconstructed(t *testing.T) {
	raw := record(func(w *builder) {
		w.header(EventCommit, 1700000006, "dc1")
		w.f64(0.001)
		w.i32(1)  // num mutations
		w.i32(20) // commit bytes
		w.i64(1)  // commit version
		w.i32(0)  // read conflict ranges
		w.i32(0)  // write conflict ranges
		w.i32(1)  // one mutation, in the optimized single-key-clear encoding
		w.u8(uint8(MutationClearRange))
		w.str("/users/k1\x00") // wire param1 = keyAfter(cleared key)
		w.str("")              // wire param2 = empty marker
		w.i64(1)               // read snapshot version
		w.bool(false)          // report conflicting keys
		w.bool(false)          // lock aware
		w.bool(false)          // no span
	})

	rec, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	m := rec.Events[0].Mutations[0]
	if m.Type != MutationClearRange {
		t.Fatalf("Type = %v", m.Type)
	}
	if m.Param1 != "/users/k1" {
		t.Errorf("Param1 = %q, want the cleared key itself", m.Param1)
	}
	if m.Param2 != "/users/k1\x00" {
		t.Errorf("Param2 = %q, want keyAfter(key)", m.Param2)
	}
}

func TestDecodeMalformedSingleKeyClear(t *testing.T) {
	raw := record(func(w *builder) {
		w.header(EventCommit, 1700000007, "dc1")
		w.f64(0.001)
		w.i32(1)
		w.i32(20)
		w.i64(1)
		w.i32(0)
		w.i32(0)
		w.i32(1)
		w.u8(uint8(MutationClearRange))
		w.str("/no-trailing-nul") // invalid for the single-key encoding
		w.str("")
		w.i64(1)
		w.bool(false)
		w.bool(false)
		w.bool(false)
	})
	if _, err := DecodeValue(raw); err == nil {
		t.Fatal("want an error for a malformed single-key clear")
	}
}

func TestEndKeyBoundsThePrefix(t *testing.T) {
	if string(KeyPrefix) >= string(EndKey) {
		t.Fatal("EndKey must sort after KeyPrefix")
	}
	// The counter key must sort outside the record range, or the tailer would
	// try to parse it as a chunk key.
	if !(string(CounterKey) >= string(EndKey)) {
		t.Fatal("CounterKey must sort at or after EndKey")
	}
}
