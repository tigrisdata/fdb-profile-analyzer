package prefix

import "testing"

func TestMatchLongestWins(t *testing.T) {
	m := New([]string{"/users", "/users/sessions", "/orders"})

	tests := []struct {
		key  string
		want string
	}{
		{"/users/sessions/abc", "/users/sessions"},
		{"/users/1", "/users"},
		{"/orders/9", "/orders"},
		{"/nothing/here", "/nothing/here (unknown)"},
		{"", Other},
	}
	for _, tt := range tests {
		if got := m.Match(tt.key); got != tt.want {
			t.Errorf("Match(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

// An unmatched key is labelled by its own leading bytes, so a production
// dashboard says where unclassified workload comes from instead of hiding it
// all behind one (other) row.
func TestUnknownKeysKeepTheirLeadingBytes(t *testing.T) {
	m := New([]string{"/users"})

	tests := []struct {
		name string
		key  string
		want string
	}{
		{"short key kept whole", "/repl_log", "/repl_log (unknown)"},
		{"long key truncated to 16 bytes", "/replication_log/shard/00042", "/replication_log (unknown)"},
		{"binary bytes escaped", "\x15\x01glob\x00tail", "\\x15\\x01glob\\x00tail (unknown)"},
	}
	for _, tt := range tests {
		if got := m.Match(tt.key); got != tt.want {
			t.Errorf("%s: Match(%q) = %q, want %q", tt.name, tt.key, got, tt.want)
		}
	}

	// The label must never collide with or match as a configured prefix: a
	// second pass over a label classifies it as its own unknown, not /users.
	if got := m.Match("/users/1"); got != "/users" {
		t.Errorf("configured prefix stopped matching: %q", got)
	}
}

// The profiling machinery profiles itself, so its own keys must not be allowed
// to dominate the hotspot rankings.
func TestSystemKeysAreBucketedSeparately(t *testing.T) {
	m := New([]string{"/users"})
	const counter = "\xff\x02/fdbClientInfo/client_latency_counter/"

	if got := m.Match(counter); got != System {
		t.Errorf("Match(profile counter) = %q, want %q", got, System)
	}
}

func TestNewNormalises(t *testing.T) {
	m := New([]string{" /users ", "/users", "", "/a"})
	got := m.Prefixes()
	if len(got) != 2 {
		t.Fatalf("Prefixes() = %v, want 2 entries", got)
	}
	if got[0] != "/users" || got[1] != "/a" {
		t.Errorf("Prefixes() = %v, want longest first", got)
	}
}
