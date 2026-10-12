package prefix

import (
	"fmt"
	"testing"
)

// Match is called once per event and once per mutation — the hottest loop in
// the sink — and is a linear scan over the configured prefixes. These
// benchmarks exist to make that cost visible before anyone ships a config with
// hundreds of prefixes: at the test bed's 3 it is nanoseconds, and the scan is
// the right simplicity; if the 200-prefix number ever matters in a profile,
// that is the moment to replace the scan with a byte-trie or a binary search
// over the sorted list, not before.
func benchmarkMatch(b *testing.B, nPrefixes int) {
	prefixes := make([]string, nPrefixes)
	for i := range prefixes {
		prefixes[i] = fmt.Sprintf("/tenant%03d/table", i)
	}
	m := New(prefixes)

	// Worst case on purpose: a key matching no prefix walks the whole list.
	keys := []string{
		prefixes[0] + "/row/1",           // best case: longest-first hits early
		prefixes[nPrefixes/2] + "/row/2", // middle
		"/nomatch/at/all",                // full scan, then an (unknown) label is built
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Match(keys[i%len(keys)])
	}
}

func BenchmarkMatch3(b *testing.B)   { benchmarkMatch(b, 3) }
func BenchmarkMatch200(b *testing.B) { benchmarkMatch(b, 200) }
