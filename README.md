# fdb-profile-analyzer

Query analytics for FoundationDB. `fdbprof` tails the client transaction profiles a
cluster records once `profile client set` is on, writes them to ClickHouse, and a
Grafana dashboard slices them: which key prefixes cost the most time, which keys and
ranges are hot, which ranges transactions conflict on, and latency percentiles per
operation. It is like Percona's Query Analytics, but for FoundationDB 7.3 and later.

`fdbprof` only reads from the cluster and caps how much it reads per poll. When it
falls behind, it gives up profile history (and the dashboard shows the loss) rather
than add load to FoundationDB.

## Examples

```bash
# Tail a cluster into ClickHouse until stopped; a restart resumes from a cursor kept in ClickHouse
fdbprof -config /etc/fdbprof/config.yaml

# Drain what is in the profile keyspace right now, then exit
fdbprof -config /etc/fdbprof/config.yaml -once

# The same daemon from the published image, with the cluster file passed in as contents
docker run -d --name fdbprof \
  -e FDB_CLUSTER_FILE_CONTENTS="$(cat /etc/foundationdb/fdb.cluster)" \
  -e FDB_CLUSTER_NAME=prod-east \
  -e CLICKHOUSE_ADDRESS=ch-1.internal:9000 \
  -e CLICKHOUSE_PASSWORD="$CH_PASSWORD" \
  -e INGESTER_ID=fdbprof-prod-east \
  -v /etc/fdbprof/config.yaml:/etc/fdbprof/config.yaml:ro \
  quay.io/tigrisdata/fdb-profile-analyzer:latest -config /etc/fdbprof/config.yaml

# No ClickHouse: decode the profile keyspace and print the hottest keys and 12-byte key regions
go run ./cmd/profdump -cluster-file /etc/foundationdb/fdb.cluster -top-keys 20 -range-depth 12
```

You can also query ClickHouse directly. This lists the most expensive prefixes over the last hour:

```sql
SELECT key_prefix, op_type,
       count()                 AS ops,
       sum(latency) / 3600     AS load,     -- average operations in flight
       quantile(0.99)(latency) AS p99_seconds
FROM fdb_profiles.operations
WHERE cluster = 'prod-east' AND event_time > now() - INTERVAL 1 HOUR
GROUP BY key_prefix, op_type
ORDER BY load DESC
LIMIT 10
```

## Setup

Setup has four steps: turn on profiling in FoundationDB, create the schema in
ClickHouse, point `fdbprof` at the cluster file, and import the dashboard into Grafana.

### 1. Turn on client profiling

```bash
fdbcli --exec 'profile client set 0.01 100MB'   # sample 1% of transactions, cap the buffer at 100MB
fdbcli --exec 'profile client get'
```

`fdbprof` never changes these settings. Every count on the dashboard comes from
transactions sampled at this rate. The size sets the cap on the ring buffer, which
FoundationDB trims oldest-first. If the buffer is too small for your traffic, or the
ingester stops, records are trimmed before `fdbprof` reads them, and the dashboard
marks the loss as a history gap.

### 2. Create the ClickHouse schema

The tables are `_local` ReplacingMergeTrees behind Distributed tables, which route
over a ClickHouse cluster named **`fdb`**. Your `remote_servers` config must define
that cluster, even on a single node:

```xml
<!-- config.d/fdb-cluster.xml: one <shard> per node -->
<clickhouse>
  <remote_servers>
    <fdb>
      <shard>
        <replica><host>ch-1.internal</host><port>9000</port><user>fdbprof</user><password>secret</password></replica>
      </shard>
    </fdb>
  </remote_servers>
</clickhouse>
```

Then, on every node (add `--host`, `--user` and `--password` as your setup needs):

```bash
clickhouse-client --query 'CREATE DATABASE IF NOT EXISTS fdb_profiles'
for f in schema/*.sql; do clickhouse-client --database fdb_profiles --multiquery < "$f"; done
```

- **Keep the database name `fdb_profiles`.** The dashboard's queries name it explicitly.
- **Re-running is safe, but nothing migrates.** The files only `CREATE ... IF NOT EXISTS`
  and never alter an existing table, so after a schema change, drop and recreate the tables.
- **Retention is set by TTLs:** operations are kept for 30 days, mutations and conflict
  ranges for 7, and profiling config for 90.
- **Every column is documented** in [docs/SCHEMA.md](docs/SCHEMA.md).

### 3. Run fdbprof against the cluster

There are three ways to get `fdbprof`:

- the `fdbprof-linux-amd64` binary from
  [releases](https://github.com/tigrisdata/fdb-profile-analyzer/releases), which needs
  FoundationDB's client package for `libfdb_c`, like any FoundationDB client;
- the image `quay.io/tigrisdata/fdb-profile-analyzer:<version>` (linux/amd64, `libfdb_c`
  included);
- `make build` from source.

The config file only needs the values you change from the defaults. [config.yaml](config.yaml)
documents every key.

```yaml
fdb:
  cluster_file: /etc/foundationdb/fdb.cluster
  cluster_name: prod-east          # the label the dashboard's cluster dropdown selects
clickhouse:
  address: ch-1.internal:9000      # native protocol
  username: fdbprof
  password: secret
prefixes:                          # your keyspace map; the dashboard groups by it
  - /users
  - /orders
  - /events
```

- **Cluster file.** `fdbprof` finds the cluster file in this order, first match wins:
  `FDB_CLUSTER_FILE_CONTENTS` (written to `$TMPDIR/fdb.cluster`, the easiest option in a
  container), then `FDB_CLUSTER_FILE`, then `fdb.cluster_file`, then FoundationDB's
  default location. `fdbprof` must be able to reach the coordinators listed in it.
- **Run one daemon per FoundationDB cluster**, each with its own `fdb.cluster_name`
  (the default is `default`). Any number of daemons can write to the same ClickHouse.
- **Give each daemon a stable ingester id.** The read cursor is stored in ClickHouse
  per cluster and ingester id, and the id defaults to the hostname. In a container, set
  `INGESTER_ID` or `-ingester-id`. Otherwise every recreated container starts over from
  the oldest record still in the buffer.
- **Prefixes** are the dashboard's main grouping. When prefixes overlap, the longest
  match wins. A key that matches no prefix is labelled with its first 16 bytes plus
  `(unknown)`, which points at the next prefix worth adding.
- **Environment variables override the file:** `FDB_CLUSTER_NAME`, `CLICKHOUSE_ADDRESS`,
  `CLICKHOUSE_PASSWORD`, `LOG_FORMAT` (`json` or `text`) and `INGESTER_ID`.
- **Two settings worth knowing:**
  - `fdb.max_poll_bytes` caps how much `fdbprof` reads from the cluster per poll
    (32MB by default).
  - `ingest.capture_mutation_values: false` keeps written values out of ClickHouse.
    Those values are application payload and may include personal data. Hotspot
    analysis only needs the keys.
- **Flags:** `-config`, `-once`, `-ingester-id`, `-verbose`, `-version`.

### 4. Add the dashboard to Grafana

1. Install the ClickHouse plugin with `grafana cli plugins install grafana-clickhouse-datasource`,
   or with `GF_INSTALL_PLUGINS=grafana-clickhouse-datasource` in Docker.
2. Add a ClickHouse data source that uses the native protocol (port 9000). A read-only
   user is enough.
3. Import [fdb-profiles.json](deploy/testenv/grafana/dashboards/fdb-profiles.json) under
   *Dashboards → New → Import*, or put it in a file-provisioned dashboards folder.

The dashboard is at `/d/fdb-profile-analyzer`. Its first dropdown picks the ClickHouse
data source and the second picks the FoundationDB cluster. It is tested on Grafana 13.0.

## Dashboard panels

The filters at the top are data source, cluster, key prefix, operation and data centre,
plus a latency-percentile picker (p99 by default) and the range depth used by *Hot
keyspace regions*. Every panel follows the filters, except the profiling-configuration
row, which applies to the whole cluster. These features appear throughout the dashboard:

- **Est. columns.** Counts cover sampled transactions only. Each *Est.* column scales its
  count up by the sample rate to estimate the cluster-wide total.
- **Activity sparklines.** Each row in a table has one, drawn across the selected time
  window, which shows whether the row is steadily hot or had a burst.
- **Hex keys.** Each key is shown both as text and as hex, because FoundationDB keys are
  arbitrary bytes.
- **History gaps.** A red annotation across every panel marks where FoundationDB trimmed
  records before `fdbprof` read them. Charts under-report around a gap.

**Profiling configuration**

- **Client sample rate:** the fraction of transactions FoundationDB is sampling, over
  time. Drag-select a window to zoom the whole dashboard to it.
- **Profile buffer:** how much of the profile buffer's size cap is in use. When usage
  nears the cap, FoundationDB starts trimming the oldest records.
- **History loss:** how many history gaps occurred in the window, and how many records
  were dropped because their chunks were trimmed before reassembly. Zero is healthy.

**Overview**

- **Operations, Operations/sec, Error rate, p99 latency:** headline numbers for the
  filtered window. The error rate is the share of events with an error code. This p99 is
  always p99 and ignores the percentile picker.

**Query analyzer**

- **Operations by prefix:** one row per prefix and operation, ranked by *Load*. Load is
  total time divided by the window length, which is the average number of these
  operations in flight, so it ranks by cost rather than by frequency. Other columns are
  count, ops/sec, total, average, p95, p99 and max latency, and errors. Clicking a prefix
  opens its recent operations.

**Throughput and latency**

- **Throughput by operation:** `get`, `get_range`, `get_version`, `commit` and the three
  `error_*` types over time. A flat zero on the errors is healthy.
- **Latency by operation:** the picked percentile per operation type, with errors
  excluded.

**Hotspots**

- **Hot read keys:** the top 50 keys by `get` count, with each key's share, a running
  cumulative share, average latency and bytes read. "The top 10 keys are 60% of reads" is
  the kind of result to look for. A flat distribution is a result too.
- **Hot write keys:** the same ranking for `SET_VALUE` and `ADD_VALUE` mutations, with
  bytes written.
- **Hot scanned ranges:** the top 25 `get_range` calls, grouped by exact begin and end
  keys, with average bytes and latency. One wide scan can cost more than thousands of
  point reads.
- **Hot keyspace regions:** reads and writes bucketed by their first `range_depth` bytes,
  which shows where traffic lands regardless of call shape. A low *Distinct keys* count
  means one hot key; a high count means a hot region. Raise the depth to zoom in.

**Conflicts**

- **Commits and conflicts:** successful commits against commits that lost a conflict
  (`error_commit`) over time.
- **Conflicting ranges:** the conflict ranges carried by losing transactions, most
  frequent first. `read` rows are what the loser had read before another transaction
  changed it, and `write` rows are what it was trying to write. The range at the top is
  where contention is worst.

**Recent operations**

- **Recent operations:** the latest 200 raw events behind the aggregates, with key,
  latency, value and range sizes, mutation count, commit bytes, error code and
  transaction id.

## Repository

| Path | What |
| --- | --- |
| [cmd/fdbprof](cmd/fdbprof) | the ingest daemon |
| [cmd/profdump](cmd/profdump) | reads, decodes and ranks profile records without ClickHouse |
| [cmd/loadgen](cmd/loadgen) | profiled load generator for the test bed |
| [schema](schema) | ClickHouse DDL, column by column in [docs/SCHEMA.md](docs/SCHEMA.md) |
| [deploy/testenv/grafana/dashboards](deploy/testenv/grafana/dashboards) | the dashboard JSON |
| [test/integration](test/integration) | end-to-end tests against the test bed |
| [docs/contributing.md](docs/contributing.md) | commit convention, pull-request checks, how releases are cut |

Every commit subject starts with `major:`, `minor:` or `patch:` (`make hooks` enforces
this locally). Each merge to `main` is released as the version those prefixes add up to:
a tag, the `fdbprof-linux-amd64` binary with checksums, and the Quay image.

## Integration testing

```bash
make testenv_up    # FoundationDB, ClickHouse and Grafana in Docker, with profiling on and the schema applied
make load_start    # continuous profiled load, plus a containerised fdbprof ingesting it
make load_logs     # follow both
```

Then open <http://localhost:3000/d/fdb-profile-analyzer>, where you are logged in as
admin automatically; the test bed's cluster is named `test`. Host-side tools use
`.testenv/fdb.cluster`. `make load_stop` stops the load, and `make testenv_down`
removes everything, including data. `make integration_test` runs the full suite on a
fresh bed and records results in `test-results/`. `make` lists every target.
