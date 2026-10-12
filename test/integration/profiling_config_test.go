//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
	"github.com/pboros/fdb-profile-analyzer/internal/loadgen"
	"github.com/pboros/fdb-profile-analyzer/internal/sink"
	"github.com/pboros/fdb-profile-analyzer/internal/tail"
)

// estimateSQL is the extrapolation the dashboard's tables use to turn sampled
// rows back into cluster-wide totals. Stated here so the arithmetic is tested
// rather than trusted; TestDashboardShipsTheTestedEstimate pins it to what the
// dashboard actually queries, so the two cannot drift apart.
const estimateSQL = `round(sum(if(sample_rate > 0, 1.0 / sample_rate, 1.0)))`

// TestProfilingConfigIsCaptured covers the read half of the feature: the
// cluster's own sample rate and size limit reach the tailer, and then the
// series the dashboard plots, without being mangled on the way.
func TestProfilingConfigIsCaptured(t *testing.T) {
	// What the cluster actually says, read independently of the tailer so the
	// test is not just comparing one code path against itself.
	want, err := fdbconfig.Read(db)
	if err != nil {
		t.Fatalf("reading profiling config: %v", err)
	}
	if !want.Enabled {
		t.Fatal("test bed is not profiling; run: fdbcli> profile client set 1.0 100MB")
	}
	if !want.RateSet || !want.SizeSet {
		t.Fatalf("profiling config keys absent from global config: %+v", want)
	}
	if want.Rate <= 0 || want.Rate > 1 {
		t.Errorf("sample rate = %v, want a fraction in (0,1]", want.Rate)
	}
	if want.SizeLimit <= 0 {
		t.Errorf("size limit = %d, want positive", want.SizeLimit)
	}

	// The tailer must hand the daemon those same values: its read is where
	// every stamped row and every plotted point comes from.
	res, err := tail.New(db, tail.Options{}).Poll(1)
	if err != nil {
		t.Fatalf("polling: %v", err)
	}
	if !res.ProfilingValid {
		t.Fatal("poll returned no profiling config")
	}
	if res.Profiling.Enabled != want.Enabled ||
		res.Profiling.Rate != want.Rate ||
		res.Profiling.SizeLimit != want.SizeLimit {
		t.Errorf("tailer read %+v, want %+v", res.Profiling, want)
	}

	// And the values must survive the round trip into ClickHouse. Ring-buffer
	// bytes ride along in the same read, so assert them here too.
	if !res.RingBytesValid {
		t.Error("poll returned no ring-buffer counter; nothing has ever been profiled?")
	}
	ingesterID := "it-cfg-" + runID
	sample := sink.ProfilingSample{
		SampledAt:      time.Now(),
		Profiling:      res.Profiling,
		RingBytes:      res.RingBytes,
		RingBytesValid: res.RingBytesValid,
	}
	if err := writer(t, []string{testPfx}).WriteProfilingConfig(context.Background(), ingesterID, sample); err != nil {
		t.Fatalf("writing profiling config: %v", err)
	}

	conn := clickhouse_(t)
	var (
		enabled, ringValid uint8
		rate               float64
		sizeLimit, ring    uint64
	)
	err = conn.QueryRow(context.Background(),
		"SELECT enabled, sample_rate, size_limit, ring_buffer_bytes, ring_buffer_valid "+
			"FROM "+chDB+".profiling_config WHERE ingester_id = ?", ingesterID).
		Scan(&enabled, &rate, &sizeLimit, &ring, &ringValid)
	if err != nil {
		t.Fatalf("reading back profiling_config: %v", err)
	}

	if enabled != 1 {
		t.Errorf("stored enabled = %d, want 1", enabled)
	}
	if rate != want.Rate {
		t.Errorf("stored sample_rate = %v, want %v", rate, want.Rate)
	}
	if sizeLimit != uint64(want.SizeLimit) {
		t.Errorf("stored size_limit = %d, want %d", sizeLimit, want.SizeLimit)
	}
	if ringValid != 1 || ring != uint64(res.RingBytes) {
		t.Errorf("stored ring buffer = %d (valid %d), want %d (valid 1)", ring, ringValid, res.RingBytes)
	}
	// The buffer cannot be using more than the cap it is capped at; if it is,
	// one of the two values is being decoded wrongly.
	if ring > sizeLimit {
		t.Errorf("ring buffer %d bytes exceeds the size limit %d", ring, sizeLimit)
	}
}

// TestSampleRateStampsRowsAndScalesCounts covers the write half: the observed
// rate lands on every row, and the dashboard's estimate inverts it back to a
// cluster-wide total.
func TestSampleRateStampsRowsAndScalesCounts(t *testing.T) {
	const rate = 0.25

	// Its own prefix, for the same reason TestHotRangeIsIdentified has one:
	// several of the suite's workloads share testPfx and land on the same
	// /hot/key, so an exact count there is only correct for whichever test
	// runs first.
	ratePfx := testPfx + "s"

	rep := generate(t, loadgen.ModeHotspot, func(c *loadgen.Config) {
		c.Prefixes = []string{ratePfx}
		c.HotOps, c.WarmOps, c.ColdKeys = 12, 5, 4
	})
	if rep.Hotspot == nil {
		t.Fatal("hotspot mode produced no report")
	}
	h := rep.Hotspot

	// Ingest as though the cluster had been sampling at a quarter rate.
	//
	// The test bed really runs at 1.0, where extrapolation is the identity and
	// proves nothing. Genuinely retuning the cluster mid-suite would prove more
	// but cost far more: the new rate reaches clients asynchronously, so every
	// other test's exact-count assertions would race the propagation in both
	// directions. Pinning the rate here tests the contract the dashboard
	// actually depends on — the rate the ingester observed reaches the row, and
	// the estimate inverts it. That the observed rate is read correctly in the
	// first place is TestProfilingConfigIsCaptured's job.
	quarter := fdbconfig.Profiling{
		Enabled: true, Rate: rate, RateSet: true,
		SizeLimit: 100_000_000, SizeSet: true,
	}

	conn := clickhouse_(t)
	ingestUntilAs(t, []string{ratePfx}, &quarter, 90*time.Second, func() error {
		got := scalar[uint64](t, conn,
			"SELECT count() FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", h.HotKey)
		if int(got) != h.HotOps {
			return fmt.Errorf("hot key reads = %d, want %d", got, h.HotOps)
		}
		return nil
	})

	// Every row carries the rate, and carries only that rate — a second value
	// would mean the stamp is coming from somewhere other than the ingester's
	// observation.
	for _, tc := range []struct{ table, where string }{
		{"operations", "op_type = 'get' AND key = ?"},
		{"mutations", "mutation_type = 'SET_VALUE' AND key = ?"},
	} {
		distinct := scalar[uint64](t, conn,
			"SELECT uniqExact(sample_rate) FROM "+chDB+"."+tc.table+" WHERE "+tc.where, h.HotKey)
		if distinct != 1 {
			t.Errorf("%s: %d distinct sample rates on the hot key, want 1", tc.table, distinct)
			continue
		}
		got := scalar[float64](t, conn,
			"SELECT any(sample_rate) FROM "+chDB+"."+tc.table+" WHERE "+tc.where, h.HotKey)
		if got != rate {
			t.Errorf("%s: stamped sample_rate = %v, want %v", tc.table, got, rate)
		}
	}

	// The estimate must invert the rate: reads sampled at 0.25 stand for four
	// times as many transactions.
	est := scalar[float64](t, conn,
		"SELECT "+estimateSQL+" FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", h.HotKey)
	if want := float64(h.HotOps) / rate; est != want {
		t.Errorf("estimated hot key reads = %v, want %v (%d sampled at %v)", est, want, h.HotOps, rate)
	}

	// The skew the hotspot panels rank on must survive extrapolation: scaling
	// by a constant rate must not reorder anything.
	estWarm := scalar[float64](t, conn,
		"SELECT "+estimateSQL+" FROM "+chDB+".operations WHERE op_type = 'get' AND key = ?", h.WarmKey)
	if estWarm >= est {
		t.Errorf("estimated warm reads %v not below estimated hot reads %v", estWarm, est)
	}
}

// TestEstimateIgnoresUnknownSampleRate pins the guard in the estimate
// expression. Rows written while profiling was off, or before the column
// existed, carry rate 0; dividing by that is how a hotspot table becomes a row
// of infinities.
func TestEstimateIgnoresUnknownSampleRate(t *testing.T) {
	conn := clickhouse_(t)

	// Two unknown rows counting once each, plus one quarter-rate row standing
	// for four.
	got := scalar[float64](t, conn,
		"SELECT "+estimateSQL+" FROM (SELECT arrayJoin([0.0, 0.0, 0.25]) AS sample_rate)")
	if got != 6 {
		t.Errorf("estimate over rates [0, 0, 0.25] = %v, want 6", got)
	}

	// Explicitly: no infinities, no NaN, whatever the mix.
	finite := scalar[uint8](t, conn,
		"SELECT isFinite("+estimateSQL+") FROM (SELECT arrayJoin([0.0, 1.0]) AS sample_rate)")
	if finite != 1 {
		t.Error("estimate is not finite when some rows have an unknown rate")
	}
}

// TestDashboardShipsTheTestedEstimate stops the tested arithmetic and the
// shipped arithmetic drifting apart. The panels are the product here; a test
// that validates an expression the dashboard no longer uses is worse than no
// test, because it reads as coverage.
func TestDashboardShipsTheTestedEstimate(t *testing.T) {
	raw, err := os.ReadFile(repoPath(t, "deploy/testenv/grafana/dashboards/fdb-profiles.json"))
	if err != nil {
		t.Fatalf("reading the dashboard: %v", err)
	}
	var dash struct {
		Panels []struct {
			ID      int    `json:"id"`
			Title   string `json:"title"`
			Targets []struct {
				RawSQL string `json:"rawSql"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("parsing the dashboard: %v", err)
	}

	found := 0
	for _, p := range dash.Panels {
		for _, tgt := range p.Targets {
			if !strings.Contains(tgt.RawSQL, `AS "Est.`) {
				continue
			}
			found++
			if !strings.Contains(tgt.RawSQL, estimateSQL) {
				t.Errorf("panel %d (%q) has an estimated column but not the tested expression %s",
					p.ID, p.Title, estimateSQL)
			}
		}
	}
	if found == 0 {
		t.Errorf("no dashboard panel has an estimated column; is %s still used?", estimateSQL)
	}
}
