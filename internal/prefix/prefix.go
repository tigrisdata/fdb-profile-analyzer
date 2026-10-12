// Package prefix classifies FoundationDB keys into the configured key prefixes
// that the dashboard groups by.
//
// FoundationDB profiles carry no equivalent of a SQL query fingerprint: the
// debug transaction identifier is optional and most applications never set one.
// The grouping dimension is therefore the operator's own knowledge of the
// keyspace layout, expressed as a list of prefixes.
package prefix

import (
	"fmt"
	"sort"
	"strings"
)

// Other is the bucket for events that carry no key at all. Parenthesised so it
// cannot collide with a real prefix.
const Other = "(other)"

// UnknownSuffix marks a key that matched no configured prefix. The label keeps
// the key's own leading bytes (see Unknown), so unclassified workload still
// says where it comes from instead of vanishing into one anonymous bucket.
const UnknownSuffix = " (unknown)"

// unknownLabelBytes is how much of an unmatched key its label keeps: enough to
// recognise the workload, few enough that keys from one workload collapse into
// one label and key_prefix stays low-cardinality.
const unknownLabelBytes = 16

// System is the bucket for FoundationDB's own system keyspace.
const System = "(system)"

// systemPrefix is where FoundationDB stores, among other things, the profile
// records themselves. The profiling machinery profiles its own maintenance
// transactions, so without this these keys dominate the hotspot rankings on an
// otherwise idle cluster.
const systemPrefix = "\xff"

// Matcher maps a key to the configured prefix that owns it.
//
// Longest match wins, so "/users/sessions" can be tracked separately from
// "/users" without ordering the config carefully.
type Matcher struct {
	prefixes []string // sorted longest first
}

// New builds a Matcher. Duplicate and empty prefixes are ignored.
func New(prefixes []string) *Matcher {
	seen := make(map[string]bool, len(prefixes))
	out := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i]) != len(out[j]) {
			return len(out[i]) > len(out[j])
		}
		return out[i] < out[j]
	})
	return &Matcher{prefixes: out}
}

// Prefixes returns the configured prefixes, longest first.
func (m *Matcher) Prefixes() []string { return m.prefixes }

// Match classifies a key. An empty key — which some events legitimately carry —
// is Other rather than a spurious match; a non-empty key matching no configured
// prefix is labelled by its own leading bytes (see Unknown).
//
// Deliberately a linear scan: it is called once per event and per mutation, but
// against the handful of prefixes a real config carries it costs nanoseconds
// (BenchmarkMatch3/200 keep the number honest). Revisit as a byte-trie only if
// a profile ever shows the 200-prefix case mattering.
func (m *Matcher) Match(key string) string {
	if key == "" {
		return Other
	}
	if strings.HasPrefix(key, systemPrefix) {
		return System
	}
	for _, p := range m.prefixes {
		if strings.HasPrefix(key, p) {
			return p
		}
	}
	return Unknown(key)
}

// Unknown labels a key that matched no configured prefix: its first 16 bytes
// (non-printable ones \xNN-escaped, since FoundationDB keys are arbitrary
// bytes) plus UnknownSuffix. The suffix is parenthesised like the other
// synthetic labels, so a label can never collide with a configured prefix.
func Unknown(key string) string {
	if len(key) > unknownLabelBytes {
		key = key[:unknownLabelBytes]
	}
	var b strings.Builder
	b.Grow(len(key) + len(UnknownSuffix))
	for i := 0; i < len(key); i++ {
		if c := key[i]; c >= 0x20 && c < 0x7f {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "\\x%02x", c)
		}
	}
	b.WriteString(UnknownSuffix)
	return b.String()
}
