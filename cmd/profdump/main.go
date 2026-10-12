// Command profdump tails the FoundationDB profile keyspace and prints what it
// finds. It is the diagnostic counterpart to the daemon: same decoder, same
// tailer, no ClickHouse.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pboros/fdb-profile-analyzer/internal/config"
	"github.com/pboros/fdb-profile-analyzer/internal/profile"
	"github.com/pboros/fdb-profile-analyzer/internal/tail"
)

func main() {
	log.SetFlags(0)

	clusterFile := flag.String("cluster-file", os.Getenv("FDB_CLUSTER_FILE"), "path to fdb.cluster")
	limit := flag.Int("limit", 10000, "maximum keys to read")
	asJSON := flag.Bool("json", false, "print each record as JSON")
	topKeys := flag.Int("top-keys", 10, "show this many hottest keys (0 disables)")
	rangeDepth := flag.Int("range-depth", 16, "bucket keys by this many leading bytes for the hot-range ranking (0 disables)")
	flag.Parse()

	fdb.MustAPIVersion(config.DefaultAPIVersion)
	db, err := fdb.OpenDatabase(*clusterFile)
	if err != nil {
		log.Fatalf("opening database: %v", err)
	}

	t := tail.New(db, tail.Options{})
	var records []*profile.Record
	var ringBytes int64
	ringValid := false
	for {
		res, err := t.Poll(*limit)
		if err != nil {
			log.Fatalf("polling: %v", err)
		}
		if res.RingBytesValid {
			ringBytes, ringValid = res.RingBytes, true
		}
		records = append(records, res.Records...)
		if res.KeysRead == 0 || len(records) >= *limit {
			break
		}
	}

	enc := json.NewEncoder(os.Stdout)
	byType := map[profile.EventType]int{}
	reads := map[string]int{}
	writes := map[string]int{}
	ranges := map[string]*rangeStat{}
	var events, mutations int

	bucket := func(key, kind string) {
		if *rangeDepth <= 0 || key == "" {
			return
		}
		b := key
		if len(b) > *rangeDepth {
			b = b[:*rangeDepth]
		}
		st := ranges[b]
		if st == nil {
			st = &rangeStat{keys: map[string]struct{}{}}
			ranges[b] = st
		}
		if kind == "read" {
			st.reads++
		} else {
			st.writes++
		}
		st.keys[key] = struct{}{}
	}

	for _, rec := range records {
		if *asJSON {
			if err := enc.Encode(rec); err != nil {
				log.Fatalf("encoding: %v", err)
			}
		}
		for _, ev := range rec.Events {
			events++
			byType[ev.Type]++
			switch ev.Type {
			case profile.EventGet:
				reads[ev.Key]++
				bucket(ev.Key, "read")
			case profile.EventGetRange:
				bucket(ev.Key, "read")
			case profile.EventCommit:
				for _, m := range ev.Mutations {
					mutations++
					if m.Type.IsWrite() {
						writes[m.Param1]++
						bucket(m.Param1, "write")
					}
				}
			}
		}
	}

	st := t.Stats()
	fmt.Printf("keys read              %d\n", st.KeysRead)
	if ringValid {
		fmt.Printf("ring buffer bytes      %d\n", ringBytes)
	}
	fmt.Printf("records complete       %d\n", st.RecordsComplete)
	fmt.Printf("records dropped        %d\n", st.RecordsDropped)
	fmt.Printf("decode errors          %d\n", st.DecodeErrors)
	fmt.Printf("unsupported protocol   %d\n", st.UnsupportedProtocol)
	fmt.Printf("events                 %d\n", events)
	fmt.Printf("mutations              %d\n", mutations)

	fmt.Println("\nevents by type")
	types := make([]profile.EventType, 0, len(byType))
	for k := range byType {
		types = append(types, k)
	}
	sort.Slice(types, func(i, j int) bool { return byType[types[i]] > byType[types[j]] })
	for _, k := range types {
		fmt.Printf("  %-16s %d\n", k, byType[k])
	}

	if *topKeys > 0 {
		printTop("hottest read keys (get)", reads, *topKeys)
		printTop("hottest write keys (SET_VALUE/ADD_VALUE)", writes, *topKeys)
	}
	printRanges(ranges, *rangeDepth, *topKeys)
}

// rangeStat accumulates traffic for one keyspace bucket.
type rangeStat struct {
	reads  int
	writes int
	keys   map[string]struct{}
}

// printRanges ranks keyspace REGIONS — keys truncated to depth bytes — by
// combined read and write traffic. The distinct-keys column separates one hot
// key (low) from a genuinely hot region (high).
func printRanges(ranges map[string]*rangeStat, depth, n int) {
	if depth <= 0 || len(ranges) == 0 || n <= 0 {
		return
	}
	type row struct {
		bucket string
		st     *rangeStat
	}
	rows := make([]row, 0, len(ranges))
	total := 0
	for b, st := range ranges {
		rows = append(rows, row{b, st})
		total += st.reads + st.writes
	}
	sort.Slice(rows, func(i, j int) bool {
		ti, tj := rows[i].st.reads+rows[i].st.writes, rows[j].st.reads+rows[j].st.writes
		if ti != tj {
			return ti > tj
		}
		return rows[i].bucket < rows[j].bucket
	})
	if len(rows) > n {
		rows = rows[:n]
	}

	fmt.Printf("\nhottest key ranges (first %d bytes) — %d total ops\n", depth, total)
	for i, r := range rows {
		ops := r.st.reads + r.st.writes
		fmt.Printf("  %2d. %-34q %5d ops (%d reads, %d writes)  %5.1f%%  %d distinct keys\n",
			i+1, r.bucket, ops, r.st.reads, r.st.writes,
			float64(ops)*100/float64(total), len(r.st.keys))
	}
}

func printTop(title string, counts map[string]int, n int) {
	if len(counts) == 0 {
		return
	}
	type row struct {
		key string
		n   int
	}
	rows := make([]row, 0, len(counts))
	total := 0
	for k, c := range counts {
		rows = append(rows, row{k, c})
		total += c
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].n != rows[j].n {
			return rows[i].n > rows[j].n
		}
		return rows[i].key < rows[j].key
	})
	if len(rows) > n {
		rows = rows[:n]
	}

	fmt.Printf("\n%s — %d total\n", title, total)
	cum := 0
	for i, r := range rows {
		cum += r.n
		fmt.Printf("  %2d. %-34s %5d  %5.1f%%  cum %5.1f%%\n",
			i+1, r.key, r.n, float64(r.n)*100/float64(total), float64(cum)*100/float64(total))
	}
}
