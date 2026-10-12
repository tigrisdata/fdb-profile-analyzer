// Command testenv drives the docker-compose test bed: waiting for services,
// verifying they are usable, and reporting what is in the profile keyspace.
//
// This replaces what began as a set of shell scripts. Talking to FoundationDB
// through the Go bindings rather than piping escaped \xff keys through fdbcli
// is both clearer and less fragile — notably, `fdbcli --timeout` is only valid
// with --exec, a trap that silently broke the shell version.
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

type command struct {
	name string
	desc string
	run  func(ctx context.Context, args []string) error
}

func main() {
	commands := []command{
		{"wait", "wait for FoundationDB, ClickHouse and Grafana to become usable", runWait},
		{"verify", "check every service is usable, including Grafana -> ClickHouse", runVerify},
		{"stats", "report what is in the FoundationDB profile keyspace", runStats},
		{"report", "summarise a `go test -json` stream and record the results", runReport},
	}

	if len(os.Args) < 2 {
		usage(commands)
		os.Exit(2)
	}
	name := os.Args[1]
	for _, c := range commands {
		if c.name == name {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			if err := c.run(ctx, os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
				os.Exit(1)
			}
			return
		}
	}
	fmt.Fprintf(os.Stderr, "unknown command %q\n\n", name)
	usage(commands)
	os.Exit(2)
}

func usage(commands []command) {
	fmt.Fprintln(os.Stderr, "usage: testenv <command> [flags]")
	fmt.Fprintln(os.Stderr, "\ncommands:")
	for _, c := range commands {
		fmt.Fprintf(os.Stderr, "  %-8s %s\n", c.name, c.desc)
	}
}

// flagSet builds a flag set carrying the endpoints every command needs.
type endpoints struct {
	clusterFile  string
	chAddress    string
	chDatabase   string
	chUser       string
	chPassword   string
	grafanaURL   string
	grafanaUser  string
	grafanaPass  string
	datasourceID string
}

func (e *endpoints) bind(fs *flag.FlagSet) {
	fs.StringVar(&e.clusterFile, "cluster-file", envOr("FDB_CLUSTER_FILE", ".testenv/fdb.cluster"), "path to fdb.cluster")
	fs.StringVar(&e.chAddress, "clickhouse", envOr("CLICKHOUSE_ADDRESS", "localhost:9000"), "ClickHouse native address")
	fs.StringVar(&e.chDatabase, "clickhouse-db", envOr("CLICKHOUSE_DB", "fdb_profiles"), "ClickHouse database")
	fs.StringVar(&e.chUser, "clickhouse-user", envOr("CLICKHOUSE_USER", "fdb"), "ClickHouse user")
	fs.StringVar(&e.chPassword, "clickhouse-password", envOr("CLICKHOUSE_PASSWORD", "fdb"), "ClickHouse password")
	fs.StringVar(&e.grafanaURL, "grafana", envOr("GRAFANA_URL", "http://localhost:3000"), "Grafana base URL")
	fs.StringVar(&e.grafanaUser, "grafana-user", envOr("GRAFANA_ADMIN_USER", "admin"), "Grafana admin user")
	fs.StringVar(&e.grafanaPass, "grafana-password", envOr("GRAFANA_ADMIN_PASSWORD", "admin"), "Grafana admin password")
	fs.StringVar(&e.datasourceID, "datasource-uid", "fdb-clickhouse", "Grafana data source uid to health-check")
}

func envOr(key, fallback string) string { return cmp.Or(os.Getenv(key), fallback) }
