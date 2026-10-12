package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/pboros/fdb-profile-analyzer/internal/config"
	"github.com/pboros/fdb-profile-analyzer/internal/fdbconfig"
	"github.com/pboros/fdb-profile-analyzer/internal/profile"
	"github.com/pboros/fdb-profile-analyzer/internal/sink"
)

var apiVersionOnce bool

func openFDB(clusterFile string) (fdb.Database, error) {
	if !apiVersionOnce {
		fdb.MustAPIVersion(config.DefaultAPIVersion)
		apiVersionOnce = true
	}
	return fdb.OpenDatabase(clusterFile)
}

// profileStats summarises the profile keyspace.
type profileStats struct {
	ChunkKeys     int
	RingBytes     int64
	HasCounter    bool
	SampleKeys    []string
	OldestVersion uint64
	NewestVersion uint64
}

func readProfileStats(db fdb.Database, limit, sample int) (profileStats, error) {
	return fdbconfig.SystemRead(db, func(tr fdb.ReadTransaction) (profileStats, error) {
		var st profileStats

		// The running byte total maintained by clients as they write and trim
		// profiles: the cheapest signal of how much headroom is left before the
		// ring buffer laps a tailer's cursor.
		if v, err := tr.Get(fdb.Key(profile.CounterKey)).Get(); err != nil {
			return st, err
		} else if len(v) == 8 {
			st.HasCounter = true
			st.RingBytes = int64(binary.LittleEndian.Uint64(v))
		}

		rows, err := tr.GetRange(
			fdb.KeyRange{Begin: fdb.Key(profile.KeyPrefix), End: fdb.Key(profile.EndKey)},
			fdb.RangeOptions{Limit: limit, Mode: fdb.StreamingModeWantAll},
		).GetSliceWithError()
		if err != nil {
			return st, err
		}

		st.ChunkKeys = len(rows)
		for i, row := range rows {
			k, err := profile.ParseKey(row.Key)
			if err != nil {
				continue
			}
			if i == 0 {
				st.OldestVersion = k.Version
			}
			st.NewestVersion = k.Version
			if len(st.SampleKeys) < sample {
				st.SampleKeys = append(st.SampleKeys,
					fmt.Sprintf("v=%d txn=%s chunk %d/%d", k.Version, k.TxnID, k.ChunkNum, k.NumChunks))
			}
		}
		return st, nil
	})
}

// openClickHouse dials exactly the way the daemon does, via sink.Open, so a
// passing check here means the daemon's connection settings work too.
func openClickHouse(ctx context.Context, e *endpoints) (driver.Conn, error) {
	return sink.Open(ctx, config.ClickHouse{
		Address:     e.chAddress,
		Database:    e.chDatabase,
		Username:    e.chUser,
		Password:    e.chPassword,
		DialTimeout: 5 * time.Second,
	})
}

// grafanaHealth asks Grafana to health-check its ClickHouse data source. This
// exercises the Grafana -> ClickHouse hop rather than merely proving both
// containers are up.
func grafanaDatasourceHealth(ctx context.Context, e *endpoints) (string, error) {
	url := fmt.Sprintf("%s/api/datasources/uid/%s/health", strings.TrimRight(e.grafanaURL, "/"), e.datasourceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(e.grafanaUser, e.grafanaPass)

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var body struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decoding response (HTTP %d): %w", resp.StatusCode, err)
	}
	if !strings.EqualFold(body.Status, "OK") {
		return body.Message, fmt.Errorf("data source unhealthy: %s", body.Message)
	}
	return body.Message, nil
}

func grafanaAPIHealth(ctx context.Context, e *endpoints) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(e.grafanaURL, "/")+"/api/health", nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
