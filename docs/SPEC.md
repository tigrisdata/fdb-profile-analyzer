# fdb-profile-analyzer — specification

Working document. Everything here comes from the design conversation plus
verification against the FoundationDB source; the **Open decisions** section is
what still needs your call.

## Goal

Grab client transaction profiles from a FoundationDB cluster, load them into
ClickHouse, and expose them through a tabular Grafana dashboard so profiles can
be sliced and diced — in the spirit of Percona Monitoring and Management's Query
Analytics, but specific to FoundationDB.

## Scope

**In scope**

- A Go **daemon** that tails FDB's profile keyspace and, whenever there is
  something new, ships it to ClickHouse in bulk, with an in-memory buffer.
- The ClickHouse schema the daemon writes.
- A Grafana dashboard providing the tabular slice-and-dice interface.

**Out of scope**

- Enabling profiling. That is the operator's job (`fdbcli> profile client set
  <rate> <size>`), as is choosing the sample rate and size cap.
- Retention/trimming of the profile keyspace. FoundationDB handles that
  server-side, driven by the configured size cap.

**Targets FDB 7.3 and above.** Older protocol versions do not need to decode.

## Data source

Profile records live under `\xff\x02/fdbClientInfo/client_latency/`. Keys are
fixed-width, 69 bytes, and a Go decoder can slice them by offset:

| Offset | Bytes | Content |
| --- | --- | --- |
| 0 | 32 | `\xff\x02/fdbClientInfo/client_latency/` |
| 32 | 10 | versionstamp: 8 bytes big-endian commit version + 2 bytes batch order |
| 42 | 1 | `/` |
| 43 | 16 | transaction id, random alphanumeric (printable) |
| 59 | 1 | `/` |
| 60 | 4 | chunk number, big-endian int32 |
| 64 | 4 | total chunks, big-endian int32 |
| 68 | 1 | `/` |

An optional trailing `<debug id>/` appears only when the client set
`DEBUG_TRANSACTION_IDENTIFIER`. **Our clients do not set it**, so it cannot be
the grouping dimension.

Values are written with `BinaryWriter{IncludeVersion()}`: an 8-byte
little-endian protocol version, then a packed stream of events. Flat
little-endian with 4-byte length prefixes — not flatbuffers. Seven event types:

| Code | Event | Notable fields |
| --- | --- | --- |
| 0 | get_version | latency, priority, read version |
| 1 | get | latency, value size, key |
| 2 | get_range | latency, range size, start/end key |
| 3 | commit | latency, mutation count, commit bytes, commit version, read/write conflict ranges, mutations, read snapshot version |
| 4 | error_get | error code, key |
| 5 | error_get_range | error code, key range |
| 6 | error_commit | error code, conflict ranges, mutations |

Every event carries a `start_timestamp` (float64 Unix epoch) and a dc id.

### Format details that bite

- **`SpanContext` is 25 bytes on 7.2+** (traceID 16 + spanID 8 + flags 1), not
  16. FoundationDB's own `transaction_profiling_analyzer.py` reads 16 and
  desynchronizes the rest of the blob whenever a commit carries a span context;
  it swallows the failure as an "invalid" record. Do not port that code as-is.
- **Mutation checksums** (7.4+, `ENABLE_MUTATION_CHECKSUM`, default off) change
  the mutation type byte and append to param2. Detect rather than corrupt.
- **Records over 100KB are chunked** (`VALUE_SIZE_LIMIT`). Chunks of one logical
  record may be written by *different* commits, so they can carry different
  versionstamps and be non-adjacent in key order. Reassembly must buffer by
  transaction id with a bound on how long partials are held.

## Daemon design

- **Tailing.** The versionstamp leads the key, so the keyspace is append-ordered.
  The cursor is simply the last key read; resume with `FirstGreaterThan(lastKey)`.
  No timestamp windows, no re-scanning, no read-side dedup.
- **Cursor durability.** Persist the cursor only after a successful ClickHouse
  insert, giving at-least-once delivery.
- **Buffering.** Accumulate decoded rows and flush to ClickHouse in bulk.
- **Lag is normal.** The key's versionstamp is the commit version of the
  *profile-writing* transaction, not the profiled one. Clients batch and flush
  every `CSI_STATUS_DELAY` (10s), so event timestamps arrive out of order
  relative to key order by at least that much.
- **Keep up or lose data.** Retention is a size-capped FIFO trimmed from the
  oldest end. The running byte counter at
  `\xff\x02/fdbClientInfo/client_latency_counter/` (8-byte little-endian) should
  be scraped as a health metric — it says how much headroom remains before the
  ring buffer laps the cursor.
- Transaction options required: `READ_SYSTEM_KEYS`, `READ_LOCK_AWARE`,
  streaming mode `WANT_ALL`. All present in the Go bindings.

## Grouping dimension

PMM groups by normalized query text. FoundationDB profiles have no equivalent,
and the debug transaction identifier is unavailable here. The decision taken:
**group by known key prefix** — a configured list of prefixes, with unmatched
keys falling into an "other" bucket.

## ClickHouse

One wide, flat **operations fact table**: a row per event, not per transaction.
Events from the same transaction share `transaction_id`, so transaction-level
views are a `GROUP BY` away, while the table stays simple enough for the
dashboard to aggregate directly.

| Column | Type | Notes |
| --- | --- | --- |
| `event_time` | `DateTime64(3)` | the event's `start_timestamp`; the dashboard's time axis |
| `op_type` | `LowCardinality(String)` | `get_version`, `get`, `get_range`, `commit`, `error_get`, `error_get_range`, `error_commit` |
| `key_prefix` | `LowCardinality(String)` | matched known prefix, else the key's first 16 bytes + ` (unknown)`; the grouping dimension |
| `transaction_id` | `FixedString(16)` | from the key; joins events of one transaction |
| `event_seq` | `UInt16` | index of the event within the record; with `transaction_id`, uniquely identifies a row |
| `versionstamp` | `UInt64` | commit version from the key — tailing order, distinct from `event_time` |
| `dc_id` | `LowCardinality(String)` | |
| `latency` | `Float64` | seconds; absent on error events |
| `key`, `end_key` | `String` | `end_key` only for ranges |
| `value_size`, `range_size` | `UInt32` | read result sizes |
| `num_mutations`, `commit_bytes` | `UInt32` | commit only |
| `read_version`, `commit_version`, `read_snapshot_version` | `Int64` | |
| `priority` | `LowCardinality(String)` | `default`, `batch`, `immediate` |
| `error_code` | `Int32` | `0` when not an error event |

`PARTITION BY toDate(event_time)` so dashboard time ranges prune whole
partitions, `ORDER BY (key_prefix, op_type, event_time)` to match how the main
table groups.

Because the daemon persists its cursor only after a successful insert, delivery
is at-least-once and a crash can replay a batch. `(transaction_id, event_seq)`
is the natural dedup key — carried in the sort key so `ReplacingMergeTree`
remains available if duplicates prove to matter in practice.

### Companion tables

**Decision: capture everything the profile record contains**, including mutation
values and both conflict-range lists, even where the first version does not
query them. Columns can be dropped later; data never recorded cannot be
recovered.

These live in their own tables rather than as nested columns on the fact table.
That keeps one row per event, makes the hotspot queries a plain `GROUP BY key`
with no `ARRAY JOIN`, and — the reason that matters most — lets the bulky data
carry its **own TTL**. Trimming mutation values to 7 days while keeping the lean
operations table for a year is then a one-line `ALTER`, which is exactly the
fine-tuning lever this decision is meant to preserve.

`mutations` — one row per mutation:

| Column | Type | Notes |
| --- | --- | --- |
| `event_time` | `DateTime64(3)` | from the parent commit event |
| `transaction_id` | `FixedString(16)` | joins back to `operations` |
| `event_seq` | `UInt16` | which commit event within the record |
| `mutation_seq` | `UInt32` | index within the mutation list |
| `mutation_type` | `LowCardinality(String)` | `SET_VALUE`, `CLEAR_RANGE`, `ADD_VALUE`, … |
| `key` | `String` | `param_one` |
| `key_prefix` | `LowCardinality(String)` | matched known prefix |
| `value` | `String` | `param_two`, in full |
| `value_size` | `UInt32` | materialised so size analysis never touches `value` |

`ORDER BY (key_prefix, key, event_time)` — identical keys sort together, which
both compresses `key` hard and makes the hot-write-key `GROUP BY key` fast.

`conflict_ranges` — one row per range, read and write sets in one table
discriminated by `kind`:

| Column | Type | Notes |
| --- | --- | --- |
| `event_time` | `DateTime64(3)` | |
| `transaction_id` | `FixedString(16)` | |
| `event_seq` | `UInt16` | |
| `range_seq` | `UInt32` | index within its list |
| `kind` | `LowCardinality(String)` | `read` or `write` |
| `begin_key`, `end_key` | `String` | |
| `key_prefix` | `LowCardinality(String)` | matched on `begin_key` |

`ORDER BY (key_prefix, kind, event_time)`. Feeds the dashboard's Conflicts
row, correlated against `error_commit` events.

### Volume, and two things to know

**The sample rate is the volume control, not the schema.** Capturing mutation
values means ClickHouse ingests roughly the cluster's write payload multiplied
by the profiling sample rate. At `profile client set 1.0` that is a full second
copy of every write; at `0.01` it is one percent. Recording everything is
affordable precisely because sampling is doing the work upstream — so the rate
is worth choosing deliberately once real volume is known.

Because it is the volume control, the rate is also recorded: on every row of
every fact table (`sample_rate`), and as a time series of its own
(`profiling_config`). Without it a count means nothing on its own — the same
number is a busy cluster at 0.01 or an idle one at 1.0 — and FoundationDB keeps
no history of the setting to reconstruct afterwards. Storing it per row is what
lets the hotspot tables show an estimated cluster-wide total beside the sampled
count; storing the series is what lets the dashboard show where the rate
changed.

**Mutation values are application payload.** Storing `param_two` in full means
ClickHouse holds a copy of whatever the application writes, which may include
personal data or secrets. That is a governance question rather than a technical
one, and worth a deliberate answer before pointing this at production. The
daemon will expose a `capture_mutation_values` config flag, defaulting to on per
the decision above, so it can be turned off without a code change or a schema
migration.

## Grafana

A sortable table in the spirit of PMM's Query Analytics, provisioned as JSON
into `deploy/testenv/grafana/dashboards/`. The existing file provider has
`allowUiUpdates` on with a 10s reload, so the dashboard can be tuned in the
browser and exported back over the file.

### Filters

Two selectors first: `$datasource` (a Grafana datasource variable every panel
follows) and `$cluster` — several FoundationDB clusters typically feed one
ClickHouse, every row carries its agent's `fdb.cluster_name`, and every panel
query is guarded by the cluster sentinel just like the other variables. Then
the data-driven multi-selects, chained off the selected cluster:
`$key_prefix`, `$op_type`, `$dc_id`, plus Grafana's built-in time range.

### Profiling configuration

Two time series at the very top, above everything else: **Client sample rate**
and **Profile buffer** (bytes used against the configured cap). They are the
context every panel below is read in, which is why they come first rather than
sitting in a diagnostics corner.

Deliberately *not* filtered by the template variables — the settings are
cluster-wide, and the point of having them here is to see them whatever else is
selected. Stepped interpolation, because the values hold until an operator
changes them; a linear line would draw a gentle ramp across a window where the
rate was in fact constant and then jumped.

There is no "only show me the high-rate data" variable. Drag-selecting a
raised-rate window on the sample-rate chart already sets the dashboard time
range, which is the same filter with no extra state to keep consistent.

Where a count is shown per key or prefix, an estimated cluster-wide total sits
beside it rather than replacing it — `Est. count` next to `Count`. Mixing a
measured number and an extrapolated one in a single column would be the kind of
quiet lie this dashboard exists to avoid.

### Main table

The centrepiece. One row per (prefix, operation), ranked so the heaviest
consumer is at the top:

| # | Prefix | Op | Load | Count | Ops/sec | Total time | Avg | p95 | p99 | Max | Errors |

**Load** is PMM's primary ranking metric and carries over directly: total time
divided by wall-clock time of the selected range, i.e. average concurrency —
"how much of the cluster is this prefix occupying". It is the default sort,
descending, because it surfaces the real cost centre rather than merely the
most frequent operation.

Sorting is native to Grafana's Table panel — every column header is clickable,
so "sortable" needs no custom work. Load, Total time and Count render with the
gauge cell mode to give PMM's proportion bars. Latency columns use seconds
units, sizes use bytes.

Backing query:

```sql
SELECT
  key_prefix,
  op_type,
  sum(latency) / $__rangeSeconds        AS load,
  count()                               AS ops,
  count() / $__rangeSeconds             AS ops_per_sec,
  sum(latency)                          AS total_time,
  avg(latency)                          AS avg_latency,
  quantile(0.95)(latency)               AS p95,
  quantile(0.99)(latency)               AS p99,
  max(latency)                          AS max_latency,
  countIf(error_code != 0)              AS errors
FROM fdb_profiles.operations
WHERE $__timeFilter(event_time)
  AND key_prefix IN ($key_prefix)
  AND op_type    IN ($op_type)
GROUP BY key_prefix, op_type
ORDER BY load DESC
```

### Supporting panels

- **Overview stats:** total operations, ops/sec, error rate, p99 latency.
- **Time series:** throughput by operation type, and latency percentiles over
  time, so a row in the table can be read in context.
- **Drill-down:** a data link on the prefix cell sets `$key_prefix`, filtering a
  detail table of recent raw operations — key, latency, sizes, error code.

### Conflicts

Below the hotspots: **Commits and conflicts** (commits vs `error_commit` per
interval — the ratio is the health signal) beside **Conflicting ranges** — the
read and write conflict ranges carried by transactions that lost, ranked by
involvement. `read` rows are what the loser had read out from under it,
`write` rows what it was trying to write; the top range is the contention
point to redesign around. This is the analysis `conflict_ranges` was recorded
for, and what turns its default-on capture cost into something an operator
can act on.

### Hotspots

A second section sitting **below** the query-analyzer table, also tabular.
Where the main table answers "which prefix is costing me", hotspots answer
"which individual keys are hot" — the skew that prefix-level aggregates hide.

Starting point, to be refined once it is in front of real data.

**Hot read keys.** Individual keys ranked by read count, with a running
cumulative share so it is obvious whether a hotspot actually exists — "the top
10 keys are 60% of all reads" is the finding; a flat distribution is also a
finding. This is the presentation FoundationDB's own analyzer uses for its
top-k output.

```sql
SELECT
  key,
  count()                                  AS ops,
  count() * 100.0 / sum(count()) OVER ()   AS pct,
  sum(count()) OVER (ORDER BY count() DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      * 100.0 / sum(count()) OVER ()       AS cumulative_pct,
  avg(latency)                             AS avg_latency,
  sum(value_size)                          AS bytes_read
FROM fdb_profiles.operations
WHERE $__timeFilter(event_time)
  AND op_type = 'get'
  AND key_prefix IN ($key_prefix)
GROUP BY key
ORDER BY ops DESC
LIMIT 50
```

**Hot read ranges.** The same idea for `get_range`, keyed on
`(key, end_key)` and showing `range_size`, since a single wide range scan can
outweigh thousands of point reads.

**Hot write keys.** Mutation keys ranked by write count. This one needs
mutation keys in ClickHouse — see Open decisions — so it lands after that call
is made. FoundationDB's analyzer counts only `SET_VALUE` and `ADD_VALUE`
mutations for this, which is a reasonable default to copy.

Both read tables are answerable from the operations table exactly as specced,
so they can be built immediately.

The test environment generates a known skew for these panels via
`make testenv_load LOAD=hotspot`: one hot key, one warm key, and a cold
background, with exact counts printed on completion so tests can assert the
resulting ranking and cumulative shares rather than merely eyeballing them.

### Noted for later

PMM renders a sparkline inside each table row. Grafana's Table panel supports a
sparkline cell type, but it needs the query to return nested frames per row,
which is awkward from ClickHouse. Treated as a stretch goal rather than a
requirement.

## Open decisions

1. **What are the known prefixes?** Needed as the default grouping config. The
   test environment currently uses `/users /orders /events` as placeholders.
   This is the last item blocking a complete schema.

## Decisions taken

- **Group by known key prefix.** The debug transaction identifier, which would
  be the closest analogue to PMM's query fingerprint, is not set by our clients.
- **Capture the full profile record**, mutation values and conflict ranges
  included, rather than only the fields the first version queries. Upstream's
  `transaction_profiling_analyzer.py` reads only `mutation.param_one` for
  `SET_VALUE`/`ADD_VALUE` and never reads values or conflict ranges at all — but
  matching that would foreclose analyses we have not thought of yet, and the
  cost is reversible in a way the data loss is not.
- **FDB 7.3+ only**, so the decoder needs no pre-7.2 `SpanContext` branch.
- **Rows are labelled with their FoundationDB cluster.** One ClickHouse
  typically serves several clusters; `cluster` (from the agent's
  `fdb.cluster_name`) leads every sort key, scopes the cursor identity so
  same-hostname agents on different clusters cannot trample each other, and
  backs the dashboard's cluster dropdown. Isolation is tested: two writers
  shipping identical records under different labels stay separable, and the
  drift guard fails any panel whose SQL loses the cluster filter.
- **One schema for one shard or many.** Every table is `_local` plus a
  Distributed front over the `fdb` cluster, even on a single node, so the two
  topologies cannot drift: the plain test bed exercises the exact SQL surface
  a sharded deployment needs. Sharding keys are deterministic from row content
  — replay dedup happens per shard, so `rand()` would silently double-count
  after any crash — the writer inserts with `insert_distributed_sync=1` to
  keep the write-then-cursor guarantee, and cross-table subqueries use
  `GLOBAL IN`. Sharding only: replication is a different axis, needs Keeper,
  and stays out of scope.
- **Shed history, never pressure the cluster — but every shed must be
  visible.** The daemon is a read-only, snapshot-reading follower with a capped
  per-poll budget (`fdb.max_poll_bytes`) and no backpressure of any kind: when
  it cannot keep up, or is not running, FoundationDB's ring buffer trims unread
  records and they are gone. That trade is deliberate — profile history is
  always worth less than production headroom — and it forecloses the whole
  family of anti-loss mechanisms (adaptive drain-harder-when-behind, buffer
  autogrow, ingest-side flow control). What is not negotiable is silence: the
  tailer detects every lap of its cursor, the daemon records it to
  `profiling_config`, and the dashboard marks the window with a history-gap
  annotation and a loss counter. A lap's size in records is unknowable, so it
  is surfaced as a gap event, never invented as a number.

## Post-review hardening (2026-09-18)

A nine-angle review of the first working pipeline produced 32 confirmed
findings; all were fixed the same day. The ones that changed behaviour:

- **Safe cursor.** The tailer's persisted cursor no longer passes chunks of
  incomplete multi-chunk records, so restarts redeliver instead of losing the
  largest commits. Unit-tested in `internal/tail`.
- **Single-key clears decode correctly.** FDB writes `tr.Clear(k)` as
  `(CLEAR_RANGE, keyAfter(k), "")` on the wire; the decoder now reconstructs it
  (upstream's Python analyzer does not). Pinned by unit + integration tests.
- **Backlogs cannot livelock the daemon.** Polls stream with a byte budget and
  keep partial progress when FDB's 5-second window runs out; "caught up" is
  now "no keys read", not "no records completed".
- **Shutdown and ClickHouse blips are survivable.** Flushes run on a detached
  context with retry/backoff; a failed flush leaves the cursor unmoved and the
  batch replays on the next start.
- **Replays dedup.** The fact tables are ReplacingMergeTree keyed on
  `(…, transaction_id, event_seq)`; the cursor's replace version is its own
  FDB versionstamp, immune to wall-clock skew.
- **The dashboard survives empty variables** (a fresh database, or dc_id on a
  test bed with no localities) via a sentinel `allValue` — the shipped plugin
  turned out not to implement the documented `$__conditionalAll` macro, which
  was verified against the live API.
- **No Prometheus-only globals.** `$__range_s` is not interpolated by the
  ClickHouse plugin (it reached the server raw); Load and ops/sec derive the
  range from the plugin's own `$__fromTime`/`$__toTime`. The throughput panel
  pivots the closed op-type set into one column per type, because the plugin
  returns grouped rows as a single long frame the timeseries panel cannot
  split.
- **Hot key ranges.** The keyspace bucketed by leading bytes (`$range_depth`
  variable) and ranked by combined read+write traffic, with a distinct-keys
  column separating a hot key from a hot region. Also in
  `profdump -range-depth`, and pinned by an integration test.
- **The daemon reports ring-buffer headroom** (`ring_buffer_bytes` in every
  flush log), read in the same transaction as the poll.
- The env-vs-file precedence contradiction was resolved the twelve-factor way:
  environment wins, and the docs now say so.

Table shapes are documented column by column in [SCHEMA.md](SCHEMA.md).

## Status

**Ingest pipeline working end to end**, validated against the live test bed:
572 operations, 1,765 mutations, 2,059 conflict ranges, zero decode errors.

| Component | State |
| --- | --- |
| Test bed (FDB 7.3.79 + ClickHouse 26.3 + Grafana 13.0.8) | done |
| `internal/profile` — key parser, record decoder | done, unit tested |
| `internal/tail` — cursor tailer, chunk reassembly | done |
| `internal/prefix` — prefix classification | done, unit tested |
| `internal/config` — YAML configuration | done |
| `internal/sink` — ClickHouse writer | done |
| `cmd/fdbprof` — the daemon | done |
| `cmd/loadgen`, `cmd/profdump`, `cmd/testenv` | done |
| `internal/fdbconfig` — global-config reader (sample rate, size limit) | done |
| Ring-buffer lap detection (tailer sensor → daemon WARN → dashboard annotation) | done |
| ClickHouse schema | done, incl. `profiling_config`, TTL retention, column codecs and the sharded/unsharded Distributed layer |
| Grafana dashboard | done, incl. drill-down, recent operations, profiling-config charts and the Conflicts row |
| Integration test suite | done, 16 tests plus the gated lap test, runnable against both ClickHouse topologies |
| Continuous load container (`make load_start`) | done |
| Schema documentation ([SCHEMA.md](SCHEMA.md)) | done |

Verified behaviours:

- Cursor resumption: a second run picks up only new records, no replay.
- Multi-chunk reassembly: a 1,500-mutation, 187KB commit spanning several
  chunks is rebuilt correctly.
- Error events: a deliberate write conflict lands as `error_commit` with
  error code 1020.
- Hotspot skew: the generated ranking matches its declared shape exactly.
- System-keyspace filtering: FoundationDB's self-profiling records are dropped.
- Profiling config capture: the rate and size limit the cluster reports reach
  the tailer and the `profiling_config` series unchanged, ring-buffer counter
  included.
- Sample-rate stamping and extrapolation: rows carry the rate the ingester
  observed, and the dashboard's estimate inverts it — with a guard test that
  the expression under test is the one the dashboard actually ships.
- Replay dedup: re-ingesting a batch from a pre-batch cursor — the crash
  between insert and cursor-save — leaves `FINAL` counts unchanged across all
  three fact tables, which is the at-least-once claim the schema rests on.
- Every shipped dashboard query executes: panel targets, template-variable
  queries and the history-gap annotation all run through Grafana's query API
  against the live schema, so SQL-schema drift fails a test instead of
  rendering an empty panel.
- The decoder is byte-tested for all seven event types, each with a trailing
  sentinel event proving the positional format stays in sync — error_commit
  including both conflict-range lists. The `+inf`-means-disabled reading of
  the sample rate and every `StoredRate` guard are unit-pinned.
- Ring-buffer laps are loud: a gated end-to-end test (`LAP_TEST=1`, the final
  step of `make integration_test`) shrinks the buffer to 500KB, trims an
  unread batch, and asserts the whole visibility chain — tailer lap detection,
  daemon warning, and the loss event the dashboard annotates — while the
  trimmed batch is demonstrably absent from ClickHouse.

Verified by hand against the live test bed, since automating it would make the
rest of the suite's exact counts race global-config propagation: retuning the
cluster with `fdbcli> profile client set 0.25 50MB` while the daemon runs
produces a new `profiling_config` row within ~4s rather than at the next tick,
and rows ingested afterwards carry `sample_rate = 0.25` — 138 sampled rows
extrapolating to 552.

### Remaining

Nothing blocking. The open question is still the real key prefixes for
`config.yaml`; the sample values are placeholders.

### Notes for whoever runs this in anger

- **Prefix matching is plain string-prefix.** A configured `/user` will also
  match `/users/...`. Longest match wins, so listing both resolves it — but a
  single loose prefix silently absorbs its extensions.
- **Sampling needs global config to have arrived.** A short-lived client that
  connects and immediately transacts will not profile its first transactions.
- **Go clients need GC pressure.** A record is queued by
  `Transaction::~Transaction`, and the Go bindings destroy the C transaction
  only from a finalizer.
- **Known scale limits, measured and deferred on purpose.** Prefix
  classification is a linear scan per key — 3.7ns at 3 prefixes, 79ns at 200
  (BenchmarkMatch3/200); a byte-trie is the fix if a profile ever shows it.
  The Recent-operations panel is a top-N sort over the whole selected window
  (its sort key leads with key_prefix, not time), so it is the first panel to
  slow down at production volume — narrow the time range, or build a rollup,
  before reworking the sort key. And the integration suite's ~2.5min wall time
  is dominated by the client's fixed 10s profile-flush cadence, one wait per
  generated workload; merging workloads would trade test independence for
  seconds, and loses.
