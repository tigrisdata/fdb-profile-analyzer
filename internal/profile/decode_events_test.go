package profile

// Byte-level coverage for the event types the original tests skipped. The
// format is positional — one mis-sized field desyncs everything after it in
// the record — so every test ends its record with a plain get event and
// asserts it decodes intact: the same trailing-event guard the span test
// established.

import "testing"

func (w *builder) getVersion(latency float64, prio Priority, readVersion int64) {
	w.header(EventGetVersion, 1700000001.0, "dc1")
	w.f64(latency)
	w.i32(int32(prio))
	w.i64(readVersion)
}

func (w *builder) getRange(latency float64, rangeSize int32, begin, end string) {
	w.header(EventGetRange, 1700000002.0, "dc1")
	w.f64(latency)
	w.i32(rangeSize)
	w.str(begin)
	w.str(end)
}

func (w *builder) errorGet(code int32, key string) {
	w.header(EventErrorGet, 1700000003.0, "dc1")
	w.i32(code)
	w.str(key)
}

func (w *builder) errorGetRange(code int32, begin, end string) {
	w.header(EventErrorGetRange, 1700000004.0, "dc1")
	w.i32(code)
	w.str(begin)
	w.str(end)
}

// errorCommit writes the failed-commit event: an error code where commit puts
// its latency/size/version trio, then the same serialised CommitTransactionRef
// body — both conflict-range lists included, which is what makes it the most
// desync-prone event in the format.
func (w *builder) errorCommit(code int32, readCR, writeCR []KeyRange, muts []Mutation) {
	w.header(EventErrorCommit, 1700000005.0, "dc1")
	w.i32(code)
	w.i32(int32(len(readCR)))
	for _, r := range readCR {
		w.str(r.Begin)
		w.str(r.End)
	}
	w.i32(int32(len(writeCR)))
	for _, r := range writeCR {
		w.str(r.Begin)
		w.str(r.End)
	}
	w.i32(int32(len(muts)))
	for _, m := range muts {
		w.u8(uint8(m.Type))
		w.str(m.Param1)
		w.str(m.Param2)
	}
	w.i64(77)     // read snapshot version
	w.bool(true)  // report conflicting keys
	w.bool(true)  // lock aware
	w.bool(false) // span absent
}

// trailingGet closes a record with a sentinel get; decodeIntact asserts it.
func (w *builder) trailingGet() { w.get(1700000009.0, 0.001, 7, "/sentinel") }

func decodeIntact(t *testing.T, raw []byte, wantEvents int) []Event {
	t.Helper()
	rec, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if len(rec.Events) != wantEvents {
		t.Fatalf("got %d events, want %d", len(rec.Events), wantEvents)
	}
	last := rec.Events[wantEvents-1]
	if last.Type != EventGet || last.Key != "/sentinel" || last.ValueSize != 7 {
		t.Fatalf("trailing sentinel corrupted — the decoder lost its place: %+v", last)
	}
	return rec.Events
}

func TestDecodeGetVersion(t *testing.T) {
	raw := record(func(w *builder) {
		w.getVersion(0.0015, PriorityBatch, 123456789)
		w.trailingGet()
	})
	ev := decodeIntact(t, raw, 2)[0]
	if ev.Type != EventGetVersion {
		t.Errorf("Type = %v", ev.Type)
	}
	if ev.Latency != 0.0015 || ev.Priority != PriorityBatch || ev.ReadVersion != 123456789 {
		t.Errorf("payload: %+v", ev)
	}
}

func TestDecodeGetRange(t *testing.T) {
	raw := record(func(w *builder) {
		w.getRange(0.02, 4096, "/users/a", "/users/z")
		w.trailingGet()
	})
	ev := decodeIntact(t, raw, 2)[0]
	if ev.Type != EventGetRange {
		t.Errorf("Type = %v", ev.Type)
	}
	if ev.Latency != 0.02 || ev.RangeSize != 4096 || ev.Key != "/users/a" || ev.EndKey != "/users/z" {
		t.Errorf("payload: %+v", ev)
	}
}

func TestDecodeErrorGet(t *testing.T) {
	raw := record(func(w *builder) {
		w.errorGet(1007, "/users/hot")
		w.trailingGet()
	})
	ev := decodeIntact(t, raw, 2)[0]
	if ev.Type != EventErrorGet {
		t.Errorf("Type = %v", ev.Type)
	}
	if ev.ErrorCode != 1007 || ev.Key != "/users/hot" {
		t.Errorf("payload: %+v", ev)
	}
	// Error events carry no latency; a nonzero one here means a field was
	// read out of place.
	if ev.Latency != 0 {
		t.Errorf("Latency = %v on an error event", ev.Latency)
	}
}

func TestDecodeErrorGetRange(t *testing.T) {
	raw := record(func(w *builder) {
		w.errorGetRange(1009, "/orders/", "/orders0")
		w.trailingGet()
	})
	ev := decodeIntact(t, raw, 2)[0]
	if ev.Type != EventErrorGetRange {
		t.Errorf("Type = %v", ev.Type)
	}
	if ev.ErrorCode != 1009 || ev.Key != "/orders/" || ev.EndKey != "/orders0" {
		t.Errorf("payload: %+v", ev)
	}
}

func TestDecodeErrorCommit(t *testing.T) {
	readCR := []KeyRange{{Begin: "/users/a", End: "/users/b"}, {Begin: "/users/x", End: "/users/y"}}
	writeCR := []KeyRange{{Begin: "/users/conflict/k", End: "/users/conflict/k\x00"}}
	muts := []Mutation{
		{Type: MutationSetValue, Param1: "/users/conflict/k", Param2: "v1"},
		{Type: MutationSetValue, Param1: "/users/other", Param2: "v2"},
	}
	raw := record(func(w *builder) {
		w.errorCommit(1020, readCR, writeCR, muts)
		w.trailingGet()
	})
	ev := decodeIntact(t, raw, 2)[0]

	if ev.Type != EventErrorCommit {
		t.Errorf("Type = %v", ev.Type)
	}
	if ev.ErrorCode != 1020 {
		t.Errorf("ErrorCode = %d, want 1020 (not_committed)", ev.ErrorCode)
	}
	if len(ev.ReadConflictRanges) != 2 ||
		ev.ReadConflictRanges[0] != readCR[0] || ev.ReadConflictRanges[1] != readCR[1] {
		t.Errorf("read conflict ranges: %+v", ev.ReadConflictRanges)
	}
	if len(ev.WriteConflictRanges) != 1 || ev.WriteConflictRanges[0] != writeCR[0] {
		t.Errorf("write conflict ranges: %+v", ev.WriteConflictRanges)
	}
	if len(ev.Mutations) != 2 || ev.Mutations[0] != muts[0] || ev.Mutations[1] != muts[1] {
		t.Errorf("mutations: %+v", ev.Mutations)
	}
	if ev.ReadSnapshotVersion != 77 || !ev.ReportConflictingKeys || !ev.LockAware {
		t.Errorf("trailing flags: snapshot=%d rck=%v la=%v",
			ev.ReadSnapshotVersion, ev.ReportConflictingKeys, ev.LockAware)
	}
	if ev.Span != nil {
		t.Errorf("Span = %+v, want nil (written absent)", ev.Span)
	}
}
