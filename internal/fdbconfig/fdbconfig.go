// Package fdbconfig reads FoundationDB's cluster-wide client profiling
// settings — what `fdbcli> profile client set <rate> <size>` writes.
//
// The values live in global config, which clients fetch asynchronously by RPC
// rather than reading on every transaction. A read here therefore reports what
// the cluster has been told, which a freshly started client may not have
// applied yet; `deploy/testenv/fdb/init.sh` documents the same lag from the
// other side.
package fdbconfig

import (
	"fmt"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// The global config keys behind `profile client set`. Reading them directly
// avoids shelling out to fdbcli just to learn whether profiling is on.
var (
	SampleRateKey = fdb.Key("\xff/globalConfig/k/config/fdb_client_info/client_txn_sample_rate")
	SizeLimitKey  = fdb.Key("\xff/globalConfig/k/config/fdb_client_info/client_txn_size_limit")
)

// Profiling is the cluster's client profiling setting.
//
// RateSet and SizeSet distinguish "the operator set this" from "the key is
// absent, so the client falls back to its built-in default". That difference
// matters to anything plotting the values over time: an unset rate is a gap,
// not a zero.
type Profiling struct {
	// Enabled reports whether sampling actually happens. `profile client set`
	// stores +inf to mean disabled, so a present key is not the same as "on".
	Enabled bool
	// Rate is the fraction of transactions sampled, 0..1.
	Rate    float64
	RateSet bool
	// SizeLimit is the cap in bytes on the profile keyspace ring buffer.
	SizeLimit int64
	SizeSet   bool
}

// SystemRead runs fn in a snapshot transaction able to read the \xff keyspace.
func SystemRead[T any](db fdb.Database, fn func(tr fdb.ReadTransaction) (T, error)) (T, error) {
	var zero T
	tr, err := db.CreateTransaction()
	if err != nil {
		return zero, err
	}
	if err := tr.Options().SetReadSystemKeys(); err != nil {
		return zero, err
	}
	if err := tr.Options().SetReadLockAware(); err != nil {
		return zero, err
	}
	return fn(tr.Snapshot())
}

// Read opens its own system-key snapshot transaction. Use ReadFrom instead
// when a suitable transaction is already open.
func Read(db fdb.Database) (Profiling, error) {
	return SystemRead(db, ReadFrom)
}

// ReadFrom reads both keys from an already-configured read transaction, which
// must have system-key access. Both gets are issued before either is waited
// on, so they cost one round trip rather than two.
func ReadFrom(tr fdb.ReadTransaction) (Profiling, error) {
	rateFut := tr.Get(SampleRateKey)
	sizeFut := tr.Get(SizeLimitKey)

	rateVal, err := rateFut.Get()
	if err != nil {
		return Profiling{}, err
	}
	sizeVal, err := sizeFut.Get()
	if err != nil {
		return Profiling{}, err
	}
	return Decode(rateVal, sizeVal)
}

// Decode interprets the raw values of the two profiling keys. Split from the
// transaction so the awkward states — absent keys, the +inf that means
// disabled, a foreign tuple shape — are testable without a cluster.
//
// A value that unpacks but holds an unexpected type is treated as unset rather
// than an error: the daemon must keep tailing through whatever a future
// FoundationDB writes there, and "unknown" is the honest reading.
func Decode(rateVal, sizeVal []byte) (Profiling, error) {
	var p Profiling

	if len(rateVal) > 0 {
		t, err := tuple.Unpack(rateVal)
		if err != nil {
			return p, fmt.Errorf("unpacking sample rate: %w", err)
		}
		if len(t) == 1 {
			if f, ok := t[0].(float64); ok {
				p.Rate, p.RateSet = f, true
				p.Enabled = !math.IsInf(f, 0) && f > 0
			}
		}
	}

	if len(sizeVal) > 0 {
		t, err := tuple.Unpack(sizeVal)
		if err != nil {
			return p, fmt.Errorf("unpacking size limit: %w", err)
		}
		if len(t) == 1 {
			if n, ok := t[0].(int64); ok {
				p.SizeLimit, p.SizeSet = n, true
			}
		}
	}
	return p, nil
}

// StoredRate is the rate as it should be recorded alongside sampled data: the
// sampling fraction when profiling is on, and 0 when it is off or unset.
// Callers stamping rows use this so that "unknown" and "disabled" both read as
// 0 and are excluded from extrapolation rather than scaling counts by infinity.
func (p Profiling) StoredRate() float64 {
	if !p.Enabled || math.IsNaN(p.Rate) || math.IsInf(p.Rate, 0) || p.Rate <= 0 {
		return 0
	}
	return p.Rate
}
