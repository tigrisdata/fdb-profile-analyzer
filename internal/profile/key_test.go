package profile

import (
	"encoding/binary"
	"testing"
)

// makeKey builds a profile key the way FoundationDB lays one out.
func makeKey(version uint64, batch uint16, txnID string, chunk, total int32, debugID string) []byte {
	k := make([]byte, 0, keyHeaderSize+len(debugID)+1)
	k = append(k, KeyPrefix...)
	k = binary.BigEndian.AppendUint64(k, version)
	k = binary.BigEndian.AppendUint16(k, batch)
	k = append(k, '/')
	k = append(k, txnID...)
	k = append(k, '/')
	k = binary.BigEndian.AppendUint32(k, uint32(chunk))
	k = binary.BigEndian.AppendUint32(k, uint32(total))
	k = append(k, '/')
	if debugID != "" {
		k = append(k, debugID...)
		k = append(k, '/')
	}
	return k
}

func TestParseKey(t *testing.T) {
	const txnID = "abcdefghij123456"
	raw := makeKey(0x0102030405060708, 3, txnID, 2, 5, "")
	if len(raw) != keyHeaderSize {
		t.Fatalf("built key is %d bytes, want %d", len(raw), keyHeaderSize)
	}

	k, err := ParseKey(raw)
	if err != nil {
		t.Fatalf("ParseKey: %v", err)
	}
	if k.Version != 0x0102030405060708 {
		t.Errorf("Version = %#x", k.Version)
	}
	if k.Batch != 3 {
		t.Errorf("Batch = %d", k.Batch)
	}
	if k.TxnID != txnID {
		t.Errorf("TxnID = %q", k.TxnID)
	}
	if k.ChunkNum != 2 || k.NumChunks != 5 {
		t.Errorf("chunk %d of %d", k.ChunkNum, k.NumChunks)
	}
	if k.Single() {
		t.Error("Single() = true for a 5-chunk record")
	}
	if k.DebugID != "" {
		t.Errorf("DebugID = %q, want empty", k.DebugID)
	}
}

func TestParseKeyWithDebugID(t *testing.T) {
	k, err := ParseKey(makeKey(7, 0, "0123456789abcdef", 1, 1, "my-app-txn"))
	if err != nil {
		t.Fatalf("ParseKey: %v", err)
	}
	if k.DebugID != "my-app-txn" {
		t.Errorf("DebugID = %q", k.DebugID)
	}
	if !k.Single() {
		t.Error("Single() = false for a 1-chunk record")
	}
}

func TestParseKeyErrors(t *testing.T) {
	valid := makeKey(1, 0, "0123456789abcdef", 1, 1, "")

	tests := []struct {
		name string
		key  []byte
	}{
		{"too short", valid[:keyHeaderSize-1]},
		{"wrong prefix", append([]byte("\xff\x02/nope/"), valid[7:]...)},
		{"chunk zero", makeKey(1, 0, "0123456789abcdef", 0, 3, "")},
		{"chunk past total", makeKey(1, 0, "0123456789abcdef", 4, 3, "")},
		{"no chunks", makeKey(1, 0, "0123456789abcdef", 1, 0, "")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseKey(tt.key); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

// Keys sort by versionstamp, which is what makes tailing a simple cursor.
func TestKeyOrderingIsByVersion(t *testing.T) {
	older := makeKey(100, 0, "aaaaaaaaaaaaaaaa", 1, 1, "")
	newer := makeKey(101, 0, "bbbbbbbbbbbbbbbb", 1, 1, "")
	if string(older) >= string(newer) {
		t.Fatal("a higher commit version must sort after a lower one")
	}
}
