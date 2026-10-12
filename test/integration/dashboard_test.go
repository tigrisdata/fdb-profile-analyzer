//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestDashboardQueriesExecute pins every query the dashboard ships — panel
// targets, template-variable queries, and the history-gap annotation — by
// running each through Grafana's own query API against the live ClickHouse.
// The dashboard is the product here: a panel whose SQL has drifted from the
// schema fails silently in the UI (an empty chart), and this is the test that
// makes it fail loudly instead.
//
// Grafana macros ($__timeFilter, $__timeInterval, $__fromTime…) are
// interpolated by the datasource plugin server-side, exactly as in the
// browser. Dashboard template variables are interpolated by the frontend, so
// the test substitutes them with their default selections.
func TestDashboardQueriesExecute(t *testing.T) {
	raw, err := os.ReadFile(repoPath(t, "deploy/testenv/grafana/dashboards/fdb-profiles.json"))
	if err != nil {
		t.Fatalf("reading the dashboard: %v", err)
	}

	var dash struct {
		Templating struct {
			List []struct {
				Name  string          `json:"name"`
				Type  string          `json:"type"`
				Query json.RawMessage `json:"query"`
			} `json:"list"`
		} `json:"templating"`
		Annotations struct {
			List []struct {
				Name   string `json:"name"`
				Target struct {
					RawSQL string `json:"rawSql"`
				} `json:"target"`
			} `json:"list"`
		} `json:"annotations"`
		Panels []struct {
			ID      int    `json:"id"`
			Type    string `json:"type"`
			Title   string `json:"title"`
			Targets []struct {
				RawSQL string `json:"rawSql"`
				Format int    `json:"format"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("parsing the dashboard: %v", err)
	}

	// The default selections a fresh page load interpolates: All for the
	// multi-selects (via the dashboard's '__all__' sentinel convention), and
	// the documented defaults for the custom variables.
	vars := strings.NewReplacer(
		"$cluster", "'__all__'",
		"$key_prefix", "'__all__'",
		"$op_type", "'__all__'",
		"$dc_id", "'__all__'",
		"$percentile", "0.99",
		"$range_depth", "16",
	)

	ran := 0
	for _, p := range dash.Panels {
		if p.Type == "row" {
			continue
		}
		if len(p.Targets) == 0 || p.Targets[0].RawSQL == "" {
			t.Errorf("panel %d (%q) has no query", p.ID, p.Title)
			continue
		}
		for i, tgt := range p.Targets {
			name := fmt.Sprintf("panel %d (%q) target %d", p.ID, p.Title, i)
			// Every query must be cluster-scoped: an unguarded panel would
			// silently blend every FoundationDB cluster feeding this
			// ClickHouse into one number.
			if !strings.Contains(tgt.RawSQL, "IN ($cluster)") {
				t.Errorf("%s is not filtered by $cluster", name)
			}
			runGrafanaQuery(t, name, vars.Replace(tgt.RawSQL), tgt.Format)
			ran++
		}
	}

	// And every datasource reference must follow the selector, or switching
	// it would leave some panels reading the old ClickHouse.
	if n := strings.Count(string(raw), `"uid": "${datasource}"`); n < 20 {
		t.Errorf("only %d datasource references follow ${datasource}; panels are pinned to a fixed data source", n)
	}

	// Import-portability: the JSON must work imported into another Grafana,
	// another folder, another uid. A top-level "id" pins it to one instance's
	// database, and a drill-down URL naming this dashboard's own /d/<uid> path
	// would point an imported copy back at the original.
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	if _, ok := meta["id"]; ok {
		t.Error(`dashboard has a top-level "id"; it must be absent for imports`)
	}
	if _, ok := meta["__requires"]; !ok {
		t.Error("dashboard lost its __requires import metadata")
	}
	if strings.Contains(string(raw), "/d/fdb-profile-analyzer") {
		t.Error("dashboard links hardcode its own uid path; drill-downs must be query-only relative URLs")
	}
	for _, v := range dash.Templating.List {
		if v.Type != "query" {
			continue
		}
		var q string
		if err := json.Unmarshal(v.Query, &q); err != nil || q == "" {
			t.Errorf("variable %q: query is not the plain string this plugin executes", v.Name)
			continue
		}
		runGrafanaQuery(t, "variable "+v.Name, vars.Replace(q), 1)
		ran++
	}
	for _, a := range dash.Annotations.List {
		if a.Target.RawSQL == "" {
			t.Errorf("annotation %q has no query", a.Name)
			continue
		}
		if !strings.Contains(a.Target.RawSQL, "IN ($cluster)") {
			t.Errorf("annotation %q is not filtered by $cluster", a.Name)
		}
		runGrafanaQuery(t, "annotation "+a.Name, vars.Replace(a.Target.RawSQL), 1)
		ran++
	}

	// A refactor that emptied the dashboard should not read as a pass.
	if ran < 15 {
		t.Errorf("only %d queries found; the dashboard has lost panels", ran)
	}
}

// runGrafanaQuery executes one rawSql through POST /api/ds/query and fails the
// test if Grafana or ClickHouse rejects it.
func runGrafanaQuery(t *testing.T, name, rawSQL string, format int) {
	t.Helper()

	now := time.Now().UnixMilli()
	body, _ := json.Marshal(map[string]any{
		"from": fmt.Sprint(now - 6*3600*1000),
		"to":   fmt.Sprint(now),
		"queries": []map[string]any{{
			"refId":         "A",
			"datasource":    map[string]string{"type": "grafana-clickhouse-datasource", "uid": "fdb-clickhouse"},
			"format":        format,
			"queryType":     "sql",
			"rawSql":        rawSQL,
			"intervalMs":    15000,
			"maxDataPoints": 300,
		}},
	})

	url := strings.TrimRight(envOr("GRAFANA_URL", "http://localhost:3000"), "/") + "/api/ds/query"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(envOr("GRAFANA_USER", "admin"), envOr("GRAFANA_PASSWORD", "admin"))

	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	defer resp.Body.Close()

	var out struct {
		Results map[string]struct {
			Error  string `json:"error"`
			Status int    `json:"status"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Errorf("%s: decoding response (HTTP %d): %v", name, resp.StatusCode, err)
		return
	}
	r, ok := out.Results["A"]
	if !ok {
		t.Errorf("%s: no result frame (HTTP %d)", name, resp.StatusCode)
		return
	}
	if r.Error != "" || (r.Status != 0 && r.Status >= 400) {
		t.Errorf("%s: %s (status %d)\nSQL:\n%s", name, r.Error, r.Status, rawSQL)
	}
}
