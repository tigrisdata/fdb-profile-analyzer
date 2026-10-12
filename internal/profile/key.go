package profile

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// KeyPrefix is where FoundationDB stores client transaction profiles.
var KeyPrefix = []byte("\xff\x02/fdbClientInfo/client_latency/")

// EndKey is the exclusive upper bound of the profile keyspace: KeyPrefix with
// its final byte incremented (FoundationDB's "strinc"). Owned here, next to the
// prefix, so no consumer re-derives it — a naive last-byte increment is wrong
// for prefixes ending in 0xFF.
var EndKey = func() []byte {
	out := append([]byte(nil), KeyPrefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	panic("profile.KeyPrefix has no incrementable byte")
}()

// CounterKey holds a running total, in bytes, of everything under KeyPrefix.
// Clients increment it as they write profiles and decrement it as they trim, so
// it is the cheapest way to see how much headroom remains in the ring buffer
// before it laps a tailer's cursor.
var CounterKey = []byte("\xff\x02/fdbClientInfo/client_latency_counter/")

// Byte offsets within a profile key. The layout is fixed-width, so the parse is
// pure slicing:
//
//	 0..32  KeyPrefix
//	32..42  versionstamp: 8-byte big-endian commit version, 2-byte batch order
//	    42  '/'
//	43..59  transaction id, 16 random alphanumeric bytes
//	    59  '/'
//	60..64  chunk number,  big-endian int32, 1-based
//	64..68  total chunks,  big-endian int32
//	    68  '/'
//	  69..  optional "<debug id>/", present only when the client set
//	        DEBUG_TRANSACTION_IDENTIFIER
const (
	offVersion    = 32
	offBatch      = 40
	offTxnID      = 43
	offChunkNum   = 60
	offNumChunks  = 64
	keyHeaderSize = 69

	txnIDLen = 16
)

// Key is a parsed profile chunk key.
type Key struct {
	// Version is the commit version of the transaction that wrote this
	// profile, not of the transaction being profiled. It orders the keyspace
	// and therefore drives tailing.
	Version uint64
	// Batch disambiguates profiles committed at the same version.
	Batch uint16
	// TxnID identifies the profiled transaction and joins its chunks.
	TxnID string
	// ChunkNum is 1-based; ChunkNum == NumChunks marks the final chunk.
	ChunkNum  int32
	NumChunks int32
	// DebugID is the client-supplied DEBUG_TRANSACTION_IDENTIFIER, empty when
	// the client did not set one.
	DebugID string
}

// Single reports whether the record fits in this one chunk.
func (k Key) Single() bool { return k.NumChunks == 1 }

// ParseKey decodes a profile chunk key.
func ParseKey(k []byte) (Key, error) {
	if len(k) < keyHeaderSize {
		return Key{}, fmt.Errorf("profile key too short: %d bytes, need at least %d", len(k), keyHeaderSize)
	}
	if !bytes.HasPrefix(k, KeyPrefix) {
		return Key{}, fmt.Errorf("key is not under %q", KeyPrefix)
	}

	key := Key{
		// Big-endian here, unlike the record body: versionstamps are ordered
		// by byte comparison, which only works big-endian.
		Version:   binary.BigEndian.Uint64(k[offVersion:offBatch]),
		Batch:     binary.BigEndian.Uint16(k[offBatch : offBatch+2]),
		TxnID:     string(k[offTxnID : offTxnID+txnIDLen]),
		ChunkNum:  int32(binary.BigEndian.Uint32(k[offChunkNum:offNumChunks])),
		NumChunks: int32(binary.BigEndian.Uint32(k[offNumChunks : offNumChunks+4])),
	}

	if key.NumChunks < 1 || key.ChunkNum < 1 || key.ChunkNum > key.NumChunks {
		return Key{}, fmt.Errorf("nonsensical chunk numbering: chunk %d of %d", key.ChunkNum, key.NumChunks)
	}

	// Anything past the fixed header is the optional debug identifier, stored
	// with a trailing separator.
	if len(k) > keyHeaderSize {
		key.DebugID = string(bytes.TrimSuffix(k[keyHeaderSize:], []byte("/")))
	}
	return key, nil
}
