package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaultsWhenEmpty(t *testing.T) {
	// An empty file and a comments-only file are both the valid "all
	// defaults" configuration, not errors.
	for _, content := range []string{"", "# nothing but comments\n"} {
		cfg, err := Load(write(t, content))
		if err != nil {
			t.Fatalf("Load(%q): %v", content, err)
		}
		if cfg.FDB.APIVersion != DefaultAPIVersion {
			t.Errorf("APIVersion = %d, want default %d", cfg.FDB.APIVersion, DefaultAPIVersion)
		}
		if cfg.ClickHouse.Address != "localhost:9000" || cfg.Ingest.ProfilingSampleInterval != 15*time.Second {
			t.Errorf("defaults not applied: %+v", cfg)
		}
	}
}

func TestLoadFileOverridesDefaults(t *testing.T) {
	cfg, err := Load(write(t, "clickhouse:\n  address: ch:9440\nfdb:\n  poll_interval: 7s\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClickHouse.Address != "ch:9440" {
		t.Errorf("Address = %q", cfg.ClickHouse.Address)
	}
	if cfg.FDB.PollInterval != 7*time.Second {
		t.Errorf("PollInterval = %v", cfg.FDB.PollInterval)
	}
	// Untouched fields keep their defaults — the file states only deltas.
	if cfg.ClickHouse.FlushRows != Default().ClickHouse.FlushRows {
		t.Errorf("FlushRows = %d, lost its default", cfg.ClickHouse.FlushRows)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	// KnownFields(true): a typo must be loud, not a silently ignored knob.
	_, err := Load(write(t, "clickhose:\n  address: oops\n"))
	if err == nil || !strings.Contains(err.Error(), "clickhose") {
		t.Fatalf("want a loud unknown-field error, got %v", err)
	}
}

func TestLoadStripsBOM(t *testing.T) {
	cfg, err := Load(write(t, "\xef\xbb\xbfclickhouse:\n  address: bom:9000\n"))
	if err != nil {
		t.Fatalf("BOM not stripped: %v", err)
	}
	if cfg.ClickHouse.Address != "bom:9000" {
		t.Errorf("Address = %q", cfg.ClickHouse.Address)
	}
}

func TestEnvWinsOverFile(t *testing.T) {
	t.Setenv("FDB_CLUSTER_FILE", "/env/fdb.cluster")
	t.Setenv("CLICKHOUSE_ADDRESS", "env:9000")
	t.Setenv("CLICKHOUSE_PASSWORD", "env-secret")
	cfg, err := Load(write(t, "fdb:\n  cluster_file: /file/fdb.cluster\nclickhouse:\n  address: file:9000\n  password: file-secret\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FDB.ClusterFile != "/env/fdb.cluster" ||
		cfg.ClickHouse.Address != "env:9000" ||
		cfg.ClickHouse.Password != "env-secret" {
		t.Errorf("environment did not win: %+v", cfg)
	}
}

func TestClusterFileContentsWinOverPath(t *testing.T) {
	// FDB_CLUSTER_FILE_CONTENTS materialises a file and overrides even an
	// explicit FDB_CLUSTER_FILE — it is the container-native injection route.
	t.Setenv("FDB_CLUSTER_FILE", "/env/fdb.cluster")
	t.Setenv("FDB_CLUSTER_FILE_CONTENTS", "docker:docker@127.0.0.1:4500")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(cfg.FDB.ClusterFile)
	if err != nil {
		t.Fatalf("materialised cluster file unreadable: %v", err)
	}
	if strings.TrimSpace(string(b)) != "docker:docker@127.0.0.1:4500" {
		t.Errorf("contents = %q", b)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(*Config)
		want   string
	}{
		{"api version", func(c *Config) { c.FDB.APIVersion = 0 }, "fdb.api_version"},
		{"batch size", func(c *Config) { c.FDB.BatchSize = 0 }, "fdb.batch_size"},
		{"poll interval", func(c *Config) { c.FDB.PollInterval = 0 }, "fdb.poll_interval"},
		{"max poll bytes", func(c *Config) { c.FDB.MaxPollBytes = -1 }, "fdb.max_poll_bytes"},
		{"address", func(c *Config) { c.ClickHouse.Address = "" }, "clickhouse.address"},
		{"database", func(c *Config) { c.ClickHouse.Database = "" }, "clickhouse.database"},
		{"flush rows", func(c *Config) { c.ClickHouse.FlushRows = 0 }, "clickhouse.flush_rows"},
		{"flush interval", func(c *Config) { c.ClickHouse.FlushInterval = 0 }, "clickhouse.flush_interval"},
		{"profiling sample interval", func(c *Config) { c.Ingest.ProfilingSampleInterval = 0 }, "ingest.profiling_sample_interval"},
		{"log format", func(c *Config) { c.Logging.Format = "xml" }, "logging.format"},
	}
	for _, tc := range cases {
		cfg := Default()
		tc.break_(&cfg)
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %v does not name %q", tc.name, err, tc.want)
		}
	}
	def_ := Default()
	if err := def_.Validate(); err != nil {
		t.Errorf("defaults do not validate: %v", err)
	}
}

func TestLogFormat(t *testing.T) {
	// Absent section: the daemon keeps its historical text output.
	if got := Default().Logging.Format; got != LogFormatText {
		t.Errorf("default format = %q, want %q", got, LogFormatText)
	}

	cfg, err := Load(write(t, "logging:\n  format: json\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Logging.Format != LogFormatJSON {
		t.Errorf("Format = %q, want %q", cfg.Logging.Format, LogFormatJSON)
	}

	// LOG_FORMAT wins over the file, like the other environment overrides.
	t.Setenv("LOG_FORMAT", "text")
	cfg, err = Load(write(t, "logging:\n  format: json\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Logging.Format != LogFormatText {
		t.Errorf("LOG_FORMAT did not win: Format = %q", cfg.Logging.Format)
	}

	// An unknown value must fail loudly, including via the environment.
	t.Setenv("LOG_FORMAT", "logfmt")
	if _, err := Load(""); err == nil || !strings.Contains(err.Error(), "logging.format") {
		t.Errorf("want a logging.format error, got %v", err)
	}
}

// TestShippedConfigLoads pins the repo's own config.yaml: every documented
// example in it must parse under KnownFields and validate.
func TestShippedConfigLoads(t *testing.T) {
	if _, err := os.Stat("../../config.yaml"); err != nil {
		t.Skip("repo config.yaml not present")
	}
	if _, err := Load("../../config.yaml"); err != nil {
		t.Fatalf("shipped config.yaml does not load: %v", err)
	}
}
