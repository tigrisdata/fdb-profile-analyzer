package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
)

// result accumulates pass/fail lines so every check runs even when an earlier
// one fails — a report of everything wrong beats stopping at the first problem.
type result struct{ failures int }

func (r *result) pass(format string, args ...any) {
	fmt.Printf("    ok    "+format+"\n", args...)
}

func (r *result) fail(format string, args ...any) {
	fmt.Printf("    FAIL  "+format+"\n", args...)
	r.failures++
}

// runWait blocks until every service is usable, or the deadline passes.
func runWait(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("wait", flag.ExitOnError)
	var e endpoints
	e.bind(fs)
	timeout := fs.Duration("timeout", 3*time.Minute, "give up after this long")
	interval := fs.Duration("interval", 2*time.Second, "time between attempts")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	checks := []struct {
		name string
		fn   func() error
	}{
		{"foundationdb", func() error {
			db, err := openFDB(e.clusterFile)
			if err != nil {
				return err
			}
			// A read version proves the database is configured and serving,
			// not merely that the process accepted a connection.
			_, err = fdbconfig.SystemRead(db, func(tr fdb.ReadTransaction) (int64, error) {
				return tr.GetReadVersion().Get()
			})
			return err
		}},
		{"clickhouse", func() error {
			conn, err := openClickHouse(ctx, &e)
			if err != nil {
				return err
			}
			defer conn.Close()
			return conn.Ping(ctx)
		}},
		{"grafana", func() error { return grafanaAPIHealth(ctx, &e) }},
	}

	for _, c := range checks {
		if err := waitFor(ctx, *interval, c.fn); err != nil {
			return fmt.Errorf("%s never became ready: %w", c.name, err)
		}
		fmt.Printf("    ready %s\n", c.name)
	}
	return nil
}

func waitFor(ctx context.Context, interval time.Duration, fn func() error) error {
	var last error
	for {
		if err := fn(); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(interval):
		}
	}
}

// runVerify checks each service is genuinely usable.
func runVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	var e endpoints
	e.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	var r result

	fmt.Println("==> FoundationDB")
	db, err := openFDB(e.clusterFile)
	if err != nil {
		r.fail("opening database: %v", err)
	} else {
		if _, err := fdbconfig.SystemRead(db, func(tr fdb.ReadTransaction) (int64, error) {
			return tr.GetReadVersion().Get()
		}); err != nil {
			r.fail("database not serving read versions: %v", err)
		} else {
			r.pass("database available")
		}

		cfg, err := fdbconfig.Read(db)
		switch {
		case err != nil:
			r.fail("reading profiling config: %v", err)
		case !cfg.Enabled:
			r.fail("client profiling is OFF — run `fdbcli> profile client set <rate> <size>`")
		default:
			r.pass("client profiling: rate=%g size_limit=%d", cfg.Rate, cfg.SizeLimit)
		}
	}

	fmt.Println("==> ClickHouse")
	conn, err := openClickHouse(ctx, &e)
	if err != nil {
		r.fail("connecting as %q: %v", e.chUser, err)
	} else {
		defer conn.Close()
		r.pass("connected as %q", e.chUser)

		for _, table := range []string{"operations", "mutations", "conflict_ranges", "ingest_cursor", "profiling_config"} {
			var n uint64
			err := conn.QueryRow(ctx,
				fmt.Sprintf("SELECT count() FROM %s.%s", e.chDatabase, table)).Scan(&n)
			if err != nil {
				r.fail("table %s.%s missing or unreadable: %v — run `make testenv_schema`", e.chDatabase, table, err)
			} else {
				r.pass("table %s.%s (%d rows)", e.chDatabase, table, n)
			}
		}
	}

	fmt.Println("==> Grafana")
	if err := grafanaAPIHealth(ctx, &e); err != nil {
		r.fail("api not reachable at %s: %v", e.grafanaURL, err)
	} else {
		r.pass("api reachable at %s", e.grafanaURL)
	}
	if msg, err := grafanaDatasourceHealth(ctx, &e); err != nil {
		r.fail("data source %q: %v", e.datasourceID, err)
	} else {
		r.pass("data source %q: %s", e.datasourceID, msg)
	}

	fmt.Println()
	if r.failures > 0 {
		return fmt.Errorf("%d check(s) failed", r.failures)
	}
	fmt.Println("All checks passed.")
	return nil
}

// runStats reports what is sitting in the profile keyspace.
func runStats(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	var e endpoints
	e.bind(fs)
	limit := fs.Int("limit", 100000, "maximum chunk keys to scan")
	sample := fs.Int("sample", 5, "how many keys to print")
	if err := fs.Parse(args); err != nil {
		return err
	}

	db, err := openFDB(e.clusterFile)
	if err != nil {
		return err
	}

	cfg, err := fdbconfig.Read(db)
	if err != nil {
		return err
	}
	st, err := readProfileStats(db, *limit, *sample)
	if err != nil {
		return err
	}

	fmt.Println("==> Client profiling configuration")
	if cfg.Enabled {
		fmt.Printf("    rate %g, size limit %d bytes\n", cfg.Rate, cfg.SizeLimit)
	} else {
		fmt.Println("    OFF — no records will be produced")
		fmt.Println("    enable with: fdbcli> profile client set 1.0 100MB")
	}

	fmt.Println("==> Profile keyspace")
	fmt.Printf("    chunk keys   %d (scan limit %d)\n", st.ChunkKeys, *limit)
	if st.HasCounter {
		fmt.Printf("    ring buffer  %d bytes used of %d (%.1f%%)\n",
			st.RingBytes, cfg.SizeLimit, percent(st.RingBytes, cfg.SizeLimit))
	} else {
		fmt.Println("    ring buffer  counter absent — nothing has ever been written")
	}
	if st.ChunkKeys > 0 {
		fmt.Printf("    versions     oldest %d, newest %d\n", st.OldestVersion, st.NewestVersion)
		fmt.Println("==> Sample keys")
		for _, k := range st.SampleKeys {
			fmt.Printf("    %s\n", k)
		}
		return nil
	}

	fmt.Println()
	fmt.Println("    No profile records. Either no profiled transactions have run,")
	fmt.Println("    or the flush window has not elapsed: clients batch profile")
	fmt.Println("    writes and flush every CSI_STATUS_DELAY (10s).")
	fmt.Println("    Note fdbcli disables its own statistics logging, so it can")
	fmt.Println("    never produce records — use `make testenv_load`.")
	return nil
}

func percent(n, of int64) float64 {
	if of <= 0 {
		return 0
	}
	return float64(n) * 100 / float64(of)
}
