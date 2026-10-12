// Package config loads the daemon's YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultAPIVersion is the FoundationDB API version every binary in this
// project selects. One authoritative copy: an FDB upgrade that bumps it must
// not leave the diagnostic tools pinned to an older version than the daemon.
const DefaultAPIVersion = 730

// Config is the whole daemon configuration.
type Config struct {
	FDB        FDB        `yaml:"fdb"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	Ingest     Ingest     `yaml:"ingest"`
	Logging    Logging    `yaml:"logging"`

	// Prefixes is the operator's map of the keyspace, and the dimension the
	// dashboard groups by. Longest match wins, so a more specific prefix can be
	// listed alongside a broader one in any order. Keys matching none of them
	// are labelled with their own first 16 bytes plus "(unknown)", so
	// unclassified workload still says where it comes from.
	Prefixes []string `yaml:"prefixes"`
}

// FDB describes the cluster to tail.
type FDB struct {
	ClusterFile string `yaml:"cluster_file"`
	// ClusterName labels every row this agent writes, and is the dashboard's
	// first grouping dimension: in production several FoundationDB clusters
	// typically feed one ClickHouse, and a count that cannot say which cluster
	// it came from is noise. It is also part of the stored cursor's identity,
	// so two agents on different clusters may share a hostname-derived
	// ingester id without trampling each other's position.
	ClusterName string `yaml:"cluster_name"`
	APIVersion  int    `yaml:"api_version"`

	// PollInterval is how long to wait after catching up before looking for
	// more. Clients flush profiles every CSI_STATUS_DELAY (10s), so polling
	// much faster than that mostly finds nothing.
	PollInterval time.Duration `yaml:"poll_interval"`
	// BatchSize is the maximum profile chunk keys read per poll.
	BatchSize int `yaml:"batch_size"`
	// MaxPartials bounds how many incomplete multi-chunk records are buffered.
	MaxPartials int `yaml:"max_partials"`
	// MaxPollBytes caps how much key+value data one poll reads from the
	// cluster. This is the knob for the pipeline's read pressure on
	// FoundationDB: turning it down slows draining, which on a busy cluster
	// trades toward more ring-buffer loss — a deliberate, visible trade, since
	// losing profile history is always preferable to leaning on production.
	// 0 means the tailer default (32MB).
	MaxPollBytes int `yaml:"max_poll_bytes"`
}

// ClickHouse describes where decoded profiles are written.
type ClickHouse struct {
	Address  string `yaml:"address"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`

	// FlushRows and FlushInterval decide when a buffered batch is written:
	// whichever comes first.
	FlushRows     int           `yaml:"flush_rows"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	DialTimeout   time.Duration `yaml:"dial_timeout"`
}

// The accepted logging.format values.
const (
	LogFormatText = "text"
	LogFormatJSON = "json"
)

// Logging controls how the daemon writes its own logs.
type Logging struct {
	// Format is the log encoding: "text" (human-oriented key=value pairs) or
	// "json" (one object per line, for log collectors).
	Format string `yaml:"format"`
}

// Ingest controls how much of each record is stored.
type Ingest struct {
	// CaptureMutationValues stores mutation values (param_two) in full.
	// Mutation values are application payload and may contain personal data or
	// secrets, so this is the switch to turn that off without a schema change.
	CaptureMutationValues bool `yaml:"capture_mutation_values"`
	// CaptureConflictRanges stores read and write conflict ranges.
	CaptureConflictRanges bool `yaml:"capture_conflict_ranges"`
	// SkipSystemKeyspace drops operations against \xff... . The profiling
	// machinery profiles its own maintenance transactions, so leaving these in
	// puts FoundationDB's own bookkeeping at the top of every hotspot ranking.
	SkipSystemKeyspace bool `yaml:"skip_system_keyspace"`
	// ProfilingSampleInterval is how often the cluster's profiling settings are
	// recorded to the profiling_config series. A change to the sample rate or
	// size limit is written immediately regardless, so this only sets how
	// often an *unchanged* setting is reaffirmed — which is what distinguishes
	// "still 1.0" from "no daemon was watching".
	ProfilingSampleInterval time.Duration `yaml:"profiling_sample_interval"`
}

// Default returns a configuration that works against the bundled test bed.
func Default() Config {
	return Config{
		FDB: FDB{
			ClusterName:  "default",
			APIVersion:   DefaultAPIVersion,
			PollInterval: 2 * time.Second,
			BatchSize:    10000,
			MaxPartials:  1000,
		},
		ClickHouse: ClickHouse{
			Address:       "localhost:9000",
			Database:      "fdb_profiles",
			Username:      "fdb",
			Password:      "fdb",
			FlushRows:     5000,
			FlushInterval: 5 * time.Second,
			DialTimeout:   10 * time.Second,
		},
		Ingest: Ingest{
			CaptureMutationValues:   true,
			CaptureConflictRanges:   true,
			SkipSystemKeyspace:      true,
			ProfilingSampleInterval: 15 * time.Second,
		},
		Logging: Logging{
			Format: LogFormatText,
		},
	}
}

// Load reads a YAML file over the defaults, so a config need only state what it
// changes. Environment variables, when set, take precedence over the file —
// standard twelve-factor ordering, so a containerised deployment can override
// a baked-in config without editing it.
func Load(path string) (Config, error) {
	cfg := Default()

	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("reading config %s: %w", path, err)
		}
		// Strip a UTF-8 BOM, which editors on some platforms add and the YAML
		// parser rejects with an unhelpful message.
		b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
		dec := yaml.NewDecoder(bytes.NewReader(b))
		dec.KnownFields(true) // a typo in a config file should be loud
		// A file with zero YAML documents — empty, or only comments — decodes
		// to io.EOF. That is a valid "all defaults" configuration, not an
		// error.
		if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
			return cfg, fmt.Errorf("parsing config %s: %w", path, err)
		}
	}

	cfg.applyEnv()
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// applyEnv applies the environment on top of the file. Environment wins:
// FDB_CLUSTER_FILE, FDB_CLUSTER_FILE_CONTENTS, FDB_CLUSTER_NAME,
// CLICKHOUSE_ADDRESS, CLICKHOUSE_PASSWORD and LOG_FORMAT, when set, override
// whatever the file says.
func (c *Config) applyEnv() {
	if v := os.Getenv("FDB_CLUSTER_FILE"); v != "" {
		c.FDB.ClusterFile = v
	}
	if v := os.Getenv("FDB_CLUSTER_NAME"); v != "" {
		c.FDB.ClusterName = v
	}
	if path, err := ClusterFileFromEnv(); err == nil && path != "" {
		c.FDB.ClusterFile = path
	}
	if v := os.Getenv("CLICKHOUSE_ADDRESS"); v != "" {
		c.ClickHouse.Address = v
	}
	if v := os.Getenv("CLICKHOUSE_PASSWORD"); v != "" {
		c.ClickHouse.Password = v
	}
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		c.Logging.Format = v
	}
}

// Validate checks the configuration is usable.
func (c *Config) Validate() error {
	var errs []error

	if c.FDB.APIVersion <= 0 {
		errs = append(errs, errors.New("fdb.api_version must be positive"))
	}
	if c.FDB.BatchSize <= 0 {
		errs = append(errs, errors.New("fdb.batch_size must be positive"))
	}
	if c.FDB.PollInterval <= 0 {
		errs = append(errs, errors.New("fdb.poll_interval must be positive"))
	}
	if c.FDB.ClusterName == "" {
		errs = append(errs, errors.New("fdb.cluster_name is required — the dashboard groups by it"))
	}
	if c.FDB.MaxPollBytes < 0 {
		errs = append(errs, errors.New("fdb.max_poll_bytes must not be negative"))
	}
	if c.ClickHouse.Address == "" {
		errs = append(errs, errors.New("clickhouse.address is required"))
	}
	if c.ClickHouse.Database == "" {
		errs = append(errs, errors.New("clickhouse.database is required"))
	}
	if c.ClickHouse.FlushRows <= 0 {
		errs = append(errs, errors.New("clickhouse.flush_rows must be positive"))
	}
	if c.ClickHouse.FlushInterval <= 0 {
		errs = append(errs, errors.New("clickhouse.flush_interval must be positive"))
	}
	if c.Ingest.ProfilingSampleInterval <= 0 {
		errs = append(errs, errors.New("ingest.profiling_sample_interval must be positive"))
	}
	switch c.Logging.Format {
	case LogFormatText, LogFormatJSON:
	default:
		errs = append(errs, fmt.Errorf("logging.format must be %q or %q, got %q",
			LogFormatText, LogFormatJSON, c.Logging.Format))
	}
	return errors.Join(errs...)
}

// ClusterFileFromEnv materialises FDB_CLUSTER_FILE_CONTENTS into a file and
// returns its path, or "" when the variable is unset. In a container the
// cluster file is most easily injected as contents — the same convention the
// FoundationDB image itself uses.
func ClusterFileFromEnv() (string, error) {
	contents := os.Getenv("FDB_CLUSTER_FILE_CONTENTS")
	if contents == "" {
		return "", nil
	}
	path := filepath.Join(os.TempDir(), "fdb.cluster")
	if err := os.WriteFile(path, []byte(contents+"\n"), 0o644); err != nil {
		return "", fmt.Errorf("writing cluster file from FDB_CLUSTER_FILE_CONTENTS: %w", err)
	}
	return path, nil
}
