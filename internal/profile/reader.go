package profile

import (
	"encoding/binary"
	"fmt"
	"math"
)

// reader walks a profile record's value. FoundationDB writes these with
// BinaryWriter, which is flat little-endian with 4-byte length prefixes — not
// flatbuffers — so every field is read positionally. There is no way to skip a
// field without losing our place in the stream, which is why the decoder has to
// understand every event type even when it only cares about some of them.
type reader struct {
	buf []byte
	off int
}

func newReader(b []byte) *reader { return &reader{buf: b} }

func (r *reader) remaining() int { return len(r.buf) - r.off }

func (r *reader) bytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative length %d at offset %d", n, r.off)
	}
	if r.off+n > len(r.buf) {
		return nil, fmt.Errorf("want %d bytes at offset %d, only %d remain", n, r.off, r.remaining())
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b, nil
}

func (r *reader) uint8() (uint8, error) {
	b, err := r.bytes(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

// bool is serialised as a single byte.
func (r *reader) bool() (bool, error) {
	v, err := r.uint8()
	return v != 0, err
}

func (r *reader) int32() (int32, error) {
	b, err := r.bytes(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b)), nil
}

func (r *reader) int64() (int64, error) {
	b, err := r.bytes(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func (r *reader) uint64() (uint64, error) {
	b, err := r.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *reader) float64() (float64, error) {
	v, err := r.uint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// lenBytes reads a StringRef: a 4-byte little-endian length then that many
// bytes. The result aliases the underlying buffer; callers that retain it past
// the life of the record must copy.
func (r *reader) lenBytes() ([]byte, error) {
	n, err := r.int32()
	if err != nil {
		return nil, err
	}
	return r.bytes(int(n))
}

// lenString is lenBytes with a copy, for values kept beyond decoding.
func (r *reader) lenString() (string, error) {
	b, err := r.lenBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}
