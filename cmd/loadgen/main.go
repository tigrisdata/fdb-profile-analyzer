// Command loadgen drives profiled traffic at a FoundationDB cluster so the
// tailer, decoder and daemon have real data to work against.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/pboros/fdb-profile-analyzer/internal/config"
	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
)

func main() {
	log.SetFlags(0)

	cfg := loadgen.Default()
	var (
		clusterFile = flag.String("cluster-file", os.Getenv("FDB_CLUSTER_FILE"), "path to fdb.cluster")
		apiVersion  = flag.Int("api-version", config.DefaultAPIVersion, "FoundationDB API version")
		mode        = flag.String("mode", loadgen.ModeAll, "basic | big | conflict | hotspot | all | continuous")
		prefixes    = flag.String("prefixes", strings.Join(cfg.Prefixes, ","), "comma-separated key prefixes")
		traceDir    = flag.String("trace-dir", "", "if set, write client trace logs here")
	)
	flag.IntVar(&cfg.Txns, "txns", cfg.Txns, "basic: transactions per prefix")
	flag.IntVar(&cfg.SetsPerTxn, "sets-per-txn", cfg.SetsPerTxn, "basic: sets per transaction")
	flag.IntVar(&cfg.BigSets, "big-sets", cfg.BigSets, "big: mutations in the single large transaction")
	flag.IntVar(&cfg.HotOps, "hot-ops", cfg.HotOps, "hotspot: operations on the hot key")
	flag.IntVar(&cfg.WarmOps, "warm-ops", cfg.WarmOps, "hotspot: operations on the warm key")
	flag.IntVar(&cfg.ColdKeys, "cold-keys", cfg.ColdKeys, "hotspot: distinct cold keys, one operation each")
	flag.DurationVar(&cfg.Warmup, "warmup", cfg.Warmup,
		"settle time after connecting, so global config reaches the client before any work starts")
	flag.DurationVar(&cfg.FlushWait, "flush-wait", cfg.FlushWait,
		"stay alive this long afterwards, so the client flushes its profile buffer")
	flag.DurationVar(&cfg.Interval, "interval", cfg.Interval, "continuous: pause between cycles")
	flag.Parse()

	cfg.Prefixes = strings.Split(*prefixes, ",")
	if len(cfg.Prefixes) == 0 || cfg.Prefixes[0] == "" {
		log.Fatal("at least one prefix is required")
	}
	cfg.Logf = func(format string, args ...any) { log.Printf("    "+format, args...) }

	if *clusterFile == "" {
		path, err := config.ClusterFileFromEnv()
		if err != nil {
			log.Fatal(err)
		}
		*clusterFile = path
	}

	fdb.MustAPIVersion(*apiVersion)
	if *traceDir != "" {
		if err := fdb.Options().SetTraceEnable(*traceDir); err != nil {
			log.Fatalf("enabling client trace logs: %v", err)
		}
		log.Printf("==> client trace logs -> %s", *traceDir)
	}

	db, err := fdb.OpenDatabase(*clusterFile)
	if err != nil {
		log.Fatalf("opening database (cluster file %q): %v", *clusterFile, err)
	}

	log.Printf("==> %s load", *mode)
	start := time.Now()

	if *mode == loadgen.ModeContinuous {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := loadgen.RunContinuous(ctx, db, cfg); err != nil {
			log.Fatalf("continuous load: %v", err)
		}
		log.Printf("==> done after %s", time.Since(start).Round(time.Second))
		return
	}

	rep, err := loadgen.Run(db, *mode, cfg)
	if err != nil {
		log.Fatalf("%s load: %v", *mode, err)
	}

	rep.Hotspot.Describe(cfg.Logf)
	log.Printf("==> done: %d commits in %s", rep.Commits, time.Since(start).Round(time.Millisecond))
}
