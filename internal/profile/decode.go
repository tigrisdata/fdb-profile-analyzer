package profile

import (
	"errors"
	"fmt"
)

// ErrUnsupportedProtocol means the record was written by a FoundationDB version
// this decoder does not understand. Callers should count these rather than
// abort: a cluster mid-upgrade can legitimately produce a mix.
type ErrUnsupportedProtocol struct {
	Version uint64
}

func (e *ErrUnsupportedProtocol) Error() string {
	return fmt.Sprintf("unsupported profile protocol version 0x%016X", e.Version)
}

// minProtocol is the oldest layout this decoder handles. The project targets
// 7.3+; 6.3 costs nothing extra because the field gates are already versioned,
// and being lenient avoids discarding data from an older straggler client.
const minProtocol = ProtocolVersion63

// DecodeValue decodes one complete profile record body — a single chunk's value
// when NumChunks is 1, otherwise the concatenation of every chunk in order.
func DecodeValue(b []byte) (*Record, error) {
	r := newReader(b)

	pv, err := r.uint64()
	if err != nil {
		return nil, fmt.Errorf("reading protocol version: %w", err)
	}
	if pv < minProtocol {
		return nil, &ErrUnsupportedProtocol{Version: pv}
	}
	p := protocol(pv)

	rec := &Record{ProtocolVersion: pv}
	for r.remaining() > 0 {
		ev, err := decodeEvent(r, p)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", len(rec.Events), err)
		}
		rec.Events = append(rec.Events, ev)
	}
	return rec, nil
}

func decodeEvent(r *reader, p protocol) (Event, error) {
	var ev Event

	raw, err := r.int32()
	if err != nil {
		return ev, fmt.Errorf("reading event type: %w", err)
	}
	if raw < 0 || raw > int32(EventErrorCommit) {
		return ev, fmt.Errorf("unknown event type %d", raw)
	}
	ev.Type = EventType(raw)

	// Every event starts with the common Event header.
	if ev.StartTimestamp, err = r.float64(); err != nil {
		return ev, fmt.Errorf("start timestamp: %w", err)
	}
	if ev.DCID, err = r.lenString(); err != nil {
		return ev, fmt.Errorf("dc id: %w", err)
	}
	if p.hasTenant() {
		// Optional<LegacyTenantName>: a presence byte, then the value. The
		// field is vestigial and in practice always absent, but it still has
		// to be consumed.
		present, err := r.bool()
		if err != nil {
			return ev, fmt.Errorf("tenant presence: %w", err)
		}
		if present {
			if ev.Tenant, err = r.lenString(); err != nil {
				return ev, fmt.Errorf("tenant: %w", err)
			}
		}
	}

	switch ev.Type {
	case EventGetVersion:
		err = decodeGetVersion(r, p, &ev)
	case EventGet:
		err = decodeGet(r, &ev)
	case EventGetRange:
		err = decodeGetRange(r, &ev)
	case EventCommit:
		err = decodeCommit(r, p, &ev)
	case EventErrorGet:
		err = decodeErrorGet(r, &ev)
	case EventErrorGetRange:
		err = decodeErrorGetRange(r, &ev)
	case EventErrorCommit:
		err = decodeErrorCommit(r, p, &ev)
	}
	if err != nil {
		return ev, fmt.Errorf("%s: %w", ev.Type, err)
	}
	return ev, nil
}

func decodeGetVersion(r *reader, p protocol, ev *Event) error {
	var err error
	if ev.Latency, err = r.float64(); err != nil {
		return err
	}
	v, err := r.int32()
	if err != nil {
		return err
	}
	ev.Priority = Priority(v)
	ev.ReadVersion, err = r.int64()
	return err
}

func decodeGet(r *reader, ev *Event) error {
	var err error
	if ev.Latency, err = r.float64(); err != nil {
		return err
	}
	if ev.ValueSize, err = r.int32(); err != nil {
		return err
	}
	ev.Key, err = r.lenString()
	return err
}

func decodeGetRange(r *reader, ev *Event) error {
	var err error
	if ev.Latency, err = r.float64(); err != nil {
		return err
	}
	if ev.RangeSize, err = r.int32(); err != nil {
		return err
	}
	if ev.Key, err = r.lenString(); err != nil {
		return err
	}
	ev.EndKey, err = r.lenString()
	return err
}

func decodeCommit(r *reader, p protocol, ev *Event) error {
	var err error
	if ev.Latency, err = r.float64(); err != nil {
		return err
	}
	if ev.NumMutations, err = r.int32(); err != nil {
		return err
	}
	if ev.CommitBytes, err = r.int32(); err != nil {
		return err
	}
	if ev.CommitVersion, err = r.int64(); err != nil {
		return err
	}
	return decodeCommitBody(r, p, ev)
}

func decodeErrorGet(r *reader, ev *Event) error {
	var err error
	if ev.ErrorCode, err = r.int32(); err != nil {
		return err
	}
	ev.Key, err = r.lenString()
	return err
}

func decodeErrorGetRange(r *reader, ev *Event) error {
	var err error
	if ev.ErrorCode, err = r.int32(); err != nil {
		return err
	}
	if ev.Key, err = r.lenString(); err != nil {
		return err
	}
	ev.EndKey, err = r.lenString()
	return err
}

func decodeErrorCommit(r *reader, p protocol, ev *Event) error {
	var err error
	if ev.ErrorCode, err = r.int32(); err != nil {
		return err
	}
	return decodeCommitBody(r, p, ev)
}

// decodeCommitBody reads the serialised CommitTransactionRef shared by commit
// and error_commit: both conflict-range lists, the mutations, and the trailing
// flags and span.
func decodeCommitBody(r *reader, p protocol, ev *Event) error {
	var err error
	if ev.ReadConflictRanges, err = decodeKeyRanges(r); err != nil {
		return fmt.Errorf("read conflict ranges: %w", err)
	}
	if ev.WriteConflictRanges, err = decodeKeyRanges(r); err != nil {
		return fmt.Errorf("write conflict ranges: %w", err)
	}
	if ev.Mutations, err = decodeMutations(r, p); err != nil {
		return fmt.Errorf("mutations: %w", err)
	}
	if ev.ReadSnapshotVersion, err = r.int64(); err != nil {
		return fmt.Errorf("read snapshot version: %w", err)
	}
	if ev.ReportConflictingKeys, err = r.bool(); err != nil {
		return fmt.Errorf("report conflicting keys: %w", err)
	}
	if p.hasLockAware() {
		if ev.LockAware, err = r.bool(); err != nil {
			return fmt.Errorf("lock aware: %w", err)
		}
		if ev.Span, err = decodeSpan(r, p); err != nil {
			return fmt.Errorf("span context: %w", err)
		}
	}
	return nil
}

func decodeKeyRanges(r *reader) ([]KeyRange, error) {
	n, err := r.int32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("negative count %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]KeyRange, 0, n)
	for i := int32(0); i < n; i++ {
		begin, err := r.lenString()
		if err != nil {
			return nil, fmt.Errorf("range %d begin: %w", i, err)
		}
		end, err := r.lenString()
		if err != nil {
			return nil, fmt.Errorf("range %d end: %w", i, err)
		}
		out = append(out, KeyRange{Begin: begin, End: end})
	}
	return out, nil
}

func decodeMutations(r *reader, p protocol) ([]Mutation, error) {
	n, err := r.int32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("negative count %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]Mutation, 0, n)
	for i := int32(0); i < n; i++ {
		code, err := r.uint8()
		if err != nil {
			return nil, fmt.Errorf("mutation %d type: %w", i, err)
		}
		if MutationType(code) >= mutationTypeCount {
			// On 7.4+ with ENABLE_MUTATION_CHECKSUM the type byte carries a
			// checksum and param2 gains a suffix, so the stream would decode
			// to garbage. Refuse loudly rather than corrupt silently.
			if p.hasMutationChecksum() {
				return nil, fmt.Errorf(
					"mutation %d: type byte %d out of range, cluster may have ENABLE_MUTATION_CHECKSUM set, which this decoder does not support",
					i, code)
			}
			return nil, fmt.Errorf("mutation %d: unknown type %d", i, code)
		}
		param1, err := r.lenString()
		if err != nil {
			return nil, fmt.Errorf("mutation %d param1: %w", i, err)
		}
		param2, err := r.lenString()
		if err != nil {
			return nil, fmt.Errorf("mutation %d param2: %w", i, err)
		}
		// FoundationDB writes a single-key clear — a ClearRange whose end is
		// keyAfter(begin) — as (type, keyAfter(begin), ""), with no protocol
		// gate (MutationRef::serialize, CommitTransaction.h). Reconstruct it
		// the way FDB's own deserializer does, or every tr.Clear(key) comes
		// out with the wrong key and an inverted range. FoundationDB's Python
		// analyzer has this bug; do not mirror it.
		if MutationType(code) == MutationClearRange && param2 == "" && param1 != "" {
			if param1[len(param1)-1] != 0x00 {
				return nil, fmt.Errorf("mutation %d: single-key clear whose begin does not end in \x00", i)
			}
			param2 = param1
			param1 = param1[:len(param1)-1]
		}
		out = append(out, Mutation{Type: MutationType(code), Param1: param1, Param2: param2})
	}
	return out, nil
}

func decodeSpan(r *reader, p protocol) (*SpanContext, error) {
	present, err := r.bool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	var sc SpanContext
	if sc.TraceIDHigh, err = r.uint64(); err != nil {
		return nil, err
	}
	if sc.TraceIDLow, err = r.uint64(); err != nil {
		return nil, err
	}
	if !p.hasOTELSpanContext() {
		// Pre-7.2 this was a bare Optional<UID> and ends here.
		return &sc, nil
	}
	if sc.SpanID, err = r.uint64(); err != nil {
		return nil, err
	}
	if sc.Flags, err = r.uint8(); err != nil {
		return nil, err
	}
	return &sc, nil
}

// IsUnsupportedProtocol reports whether err came from a record this decoder
// cannot read, as opposed to a genuinely malformed one.
func IsUnsupportedProtocol(err error) bool {
	var e *ErrUnsupportedProtocol
	return errors.As(err, &e)
}
