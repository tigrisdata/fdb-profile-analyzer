package fdbconfig

import (
	"math"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func packF(f float64) []byte { return tuple.Tuple{f}.Pack() }
func packI(n int64) []byte   { return tuple.Tuple{n}.Pack() }

func TestDecode(t *testing.T) {
	cases := []struct {
		name    string
		rate    []byte
		size    []byte
		want    Profiling
		wantErr bool
	}{
		{
			// What `profile client set 1.0 100MB` writes.
			name: "enabled",
			rate: packF(1.0), size: packI(100_000_000),
			want: Profiling{Enabled: true, Rate: 1.0, RateSet: true, SizeLimit: 100_000_000, SizeSet: true},
		},
		{
			name: "fractional rate",
			rate: packF(0.01), size: packI(50_000_000),
			want: Profiling{Enabled: true, Rate: 0.01, RateSet: true, SizeLimit: 50_000_000, SizeSet: true},
		},
		{
			// `profile client set default <size>` stores +inf: the key is
			// present but profiling is OFF. The single most important case —
			// misreading it as enabled would scale every dashboard estimate
			// by infinity.
			name: "explicitly disabled via +inf",
			rate: packF(math.Inf(1)), size: packI(100_000_000),
			want: Profiling{Enabled: false, Rate: math.Inf(1), RateSet: true, SizeLimit: 100_000_000, SizeSet: true},
		},
		{
			name: "zero rate is off",
			rate: packF(0), size: packI(100_000_000),
			want: Profiling{Enabled: false, Rate: 0, RateSet: true, SizeLimit: 100_000_000, SizeSet: true},
		},
		{
			// A cluster that has never run `profile client set`.
			name: "both keys absent",
			rate: nil, size: nil,
			want: Profiling{},
		},
		{
			name: "rate set, size absent",
			rate: packF(0.5), size: nil,
			want: Profiling{Enabled: true, Rate: 0.5, RateSet: true},
		},
		{
			// A future FDB writing something else here must read as unknown,
			// not as an error that stops the daemon or a bogus number.
			name: "unexpected tuple type is unset",
			rate: packI(1), size: packF(1.5),
			want: Profiling{},
		},
		{
			name: "unexpected tuple arity is unset",
			rate: tuple.Tuple{1.0, 2.0}.Pack(), size: packI(1),
			want: Profiling{SizeLimit: 1, SizeSet: true},
		},
		{
			name:    "garbage bytes are an error",
			rate:    []byte{0xff, 0xff, 0xff},
			size:    packI(1),
			wantErr: true,
		},
	}

	for _, tc := range cases {
		got, err := Decode(tc.rate, tc.size)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: no error", tc.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s:\n got %+v\nwant %+v", tc.name, got, tc.want)
		}
	}
}

// TestStoredRate pins the guard that keeps disabled/unknown states out of the
// extrapolation arithmetic: everything that is not a real sampling fraction
// stores as 0, which the dashboard reads as "count once, do not scale".
func TestStoredRate(t *testing.T) {
	cases := []struct {
		name string
		p    Profiling
		want float64
	}{
		{"enabled fraction", Profiling{Enabled: true, Rate: 0.25}, 0.25},
		{"full rate", Profiling{Enabled: true, Rate: 1}, 1},
		{"disabled", Profiling{Enabled: false, Rate: 0.25}, 0},
		{"inf", Profiling{Enabled: true, Rate: math.Inf(1)}, 0},
		{"nan", Profiling{Enabled: true, Rate: math.NaN()}, 0},
		{"negative", Profiling{Enabled: true, Rate: -0.5}, 0},
		{"zero value", Profiling{}, 0},
	}
	for _, tc := range cases {
		if got := tc.p.StoredRate(); got != tc.want {
			t.Errorf("%s: StoredRate = %v, want %v", tc.name, got, tc.want)
		}
	}
}
