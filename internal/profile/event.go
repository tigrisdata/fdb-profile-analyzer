package profile

import "fmt"

// Protocol versions, as written into the head of every profile record. Field
// layout changed across releases, so decoding is gated on these.
const (
	ProtocolVersion62 uint64 = 0x0FDB00B062010001
	ProtocolVersion63 uint64 = 0x0FDB00B063010001
	ProtocolVersion71 uint64 = 0x0FDB00B071010000
	ProtocolVersion72 uint64 = 0x0FDB00B072000000
	ProtocolVersion73 uint64 = 0x0FDB00B073000000
	ProtocolVersion74 uint64 = 0x0FDB00B074000000
)

// protocol answers the feature questions the decoder needs. Named after the
// predicates in ProtocolVersion.h so the mapping stays obvious. Fields that
// stopped varying before minProtocol (dc id, priority, read/commit version,
// the conflicting-keys flag — all settled by 6.3) are read unconditionally.
type protocol uint64

// Tenant and lock-aware arrived together in 7.1.
func (p protocol) hasTenant() bool    { return uint64(p) >= ProtocolVersion71 }
func (p protocol) hasLockAware() bool { return uint64(p) >= ProtocolVersion71 }

// Before 7.2 the trailing span was an Optional<UID>: 16 bytes. From 7.2 it is
// an Optional<SpanContext>: 16-byte trace id, 8-byte span id, 1-byte flags.
// Reading 16 where 25 is meant desynchronises every later event in the record,
// which is the bug in FoundationDB's own transaction_profiling_analyzer.py.
func (p protocol) hasOTELSpanContext() bool { return uint64(p) >= ProtocolVersion72 }

func (p protocol) hasMutationChecksum() bool { return uint64(p) >= ProtocolVersion74 }

// EventType discriminates the seven kinds of record entry.
type EventType uint8

const (
	EventGetVersion EventType = iota
	EventGet
	EventGetRange
	EventCommit
	EventErrorGet
	EventErrorGetRange
	EventErrorCommit
)

var eventNames = [...]string{
	"get_version", "get", "get_range", "commit",
	"error_get", "error_get_range", "error_commit",
}

func (e EventType) String() string {
	if int(e) < len(eventNames) {
		return eventNames[e]
	}
	return fmt.Sprintf("event(%d)", uint8(e))
}

// Priority mirrors FdbClientLogEvents::TransactionPriorityType. Note these
// values disagree with the priority enum used elsewhere in FoundationDB.
type Priority int32

const (
	PriorityDefault   Priority = 0
	PriorityBatch     Priority = 1
	PriorityImmediate Priority = 2
	PriorityUnset     Priority = 3
)

func (p Priority) String() string {
	switch p {
	case PriorityDefault:
		return "default"
	case PriorityBatch:
		return "batch"
	case PriorityImmediate:
		return "immediate"
	default:
		return "unset"
	}
}

// MutationType mirrors MutationRef::Type.
type MutationType uint8

const (
	MutationSetValue MutationType = iota
	MutationClearRange
	MutationAddValue
	MutationDebugKeyRange
	MutationDebugKey
	MutationNoOp
	MutationAnd
	MutationOr
	MutationXor
	MutationAppendIfFits
	MutationAvailableForReuse
	MutationReservedForLogProtocolMessage
	MutationMax
	MutationMin
	MutationSetVersionstampedKey
	MutationSetVersionstampedValue
	MutationByteMin
	MutationByteMax
	MutationMinV2
	MutationAndV2
	MutationCompareAndClear

	mutationTypeCount
)

var mutationNames = [...]string{
	"SET_VALUE", "CLEAR_RANGE", "ADD_VALUE", "DEBUG_KEY_RANGE", "DEBUG_KEY",
	"NO_OP", "AND", "OR", "XOR", "APPEND_IF_FITS", "AVAILABLE_FOR_REUSE",
	"RESERVED_FOR_LOG_PROTOCOL_MESSAGE", "MAX", "MIN", "SET_VERSIONSTAMPED_KEY",
	"SET_VERSIONSTAMPED_VALUE", "BYTE_MIN", "BYTE_MAX", "MIN_V2", "AND_V2",
	"COMPARE_AND_CLEAR",
}

func (m MutationType) String() string {
	if int(m) < len(mutationNames) {
		return mutationNames[m]
	}
	return fmt.Sprintf("MUTATION(%d)", uint8(m))
}

// IsWrite reports whether the mutation places a value at a single key, which is
// what the hot-write-key ranking counts. FoundationDB's own analyzer uses
// exactly this pair.
func (m MutationType) IsWrite() bool {
	return m == MutationSetValue || m == MutationAddValue
}

// Mutation is one entry of a commit's mutation list.
type Mutation struct {
	Type MutationType
	// Param1 is the key for most mutation types, or the range begin for
	// CLEAR_RANGE.
	Param1 string
	// Param2 is the value, or the range end for CLEAR_RANGE.
	Param2 string
}

// KeyRange is a half-open [Begin, End) interval.
type KeyRange struct {
	Begin string
	End   string
}

// SpanContext is the OpenTelemetry span attached to a commit, when present.
type SpanContext struct {
	TraceIDHigh uint64
	TraceIDLow  uint64
	SpanID      uint64
	Flags       uint8
}

// Event is one entry in a profile record. Fields not meaningful for a given
// Type are left zero; Type says which to read.
type Event struct {
	Type           EventType
	StartTimestamp float64 // Unix epoch seconds, from the client's clock
	DCID           string
	Tenant         string

	Latency float64 // seconds; not set on error events

	Priority    Priority // get_version
	ReadVersion int64    // get_version

	ValueSize int32 // get
	RangeSize int32 // get_range

	Key    string // get, error_get, get_range begin, error_get_range begin
	EndKey string // get_range, error_get_range

	NumMutations          int32 // commit
	CommitBytes           int32 // commit
	CommitVersion         int64 // commit
	ReadSnapshotVersion   int64 // commit, error_commit
	ReportConflictingKeys bool
	LockAware             bool
	Span                  *SpanContext

	ReadConflictRanges  []KeyRange
	WriteConflictRanges []KeyRange
	Mutations           []Mutation

	ErrorCode int32 // error events
}

// Record is one decoded transaction profile: the key it was stored under, the
// protocol version that wrote it, and its events in order.
type Record struct {
	Key             Key
	ProtocolVersion uint64
	Events          []Event
}
