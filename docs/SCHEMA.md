# ClickHouse table shapes

What the daemon writes, column by column, and why the tables are shaped the way
they are. The DDL lives in
[schema/](../schema/); this
document is the contract behind it.

One decoded profile record — one FoundationDB transaction — fans out into:

```
record ─┬─► operations       one row per event        (always)
        ├─► mutations        one row per mutation     (from commit / error_commit)
        └─► conflict_ranges  one row per range        (when capture_conflict_ranges)
```

plus `ingest_cursor`, the daemon's own bookkeeping.

## Conventions shared by all tables

- **Delivery is at-least-once.** The daemon writes rows first and saves its
  cursor second, so a crash between the two replays a batch. All three fact
  tables are therefore `ReplacingMergeTree`: replayed rows are identical in
  every sort-key column and background merges collapse them. Dedup is
  *eventual* — a count taken moments after a crash can briefly include
  duplicates; append `FINAL` to a query when exactness matters more than speed.
- **`(transaction_id, event_seq)` identifies an event** across all tables.
  `transaction_id` is the 16-byte random id from the profile key
  (`FixedString(16)`); `event_seq` (`UInt32`) is the event's position inside
  its record. `mutation_seq`/`range_seq` extend the identity one level down.
- **`event_time` is the client's clock, not the cluster's.** It is the event's
  own `start_timestamp` and the dashboard's time axis. It is *not* the order
  the tailer saw records in — clients batch profiles and flush every 10s, so
  rows arrive out of time order by at least that much.
- **`versionstamp` is the tailer's order**: the commit version of the
  transaction that *wrote the profile*, taken from the key. Use it to correlate
  with cursor positions; never as a time axis.
- **Partitioning** is `toDate(event_time)` everywhere, so dashboard time ranges
  prune whole partitions.
- **Every row says which FoundationDB cluster it came from.** `cluster`
  (`LowCardinality(String)`) is the label the agent is configured with
  (`fdb.cluster_name`, env `FDB_CLUSTER_NAME`; `test` on the bundled test
  bed). In production several clusters feed one ClickHouse, so it leads every
  sort key, scopes the stored cursor's identity, and is the dashboard's
  second dropdown (after the data-source selector); every panel carries
  `AND (cluster IN ($cluster) OR '__all__' IN ($cluster))`, and an integration
  test proves the guard isolates one cluster's rows when two report the same
  keys.
- **Every table is a pair: `<name>_local` plus a Distributed front named
  `<name>`.** The `_local` table is the real ReplacingMergeTree — the engine
  blocks below describe it — and the same-named Distributed table over the
  `fdb` cluster is what the writer inserts into and every query reads. The
  cluster has one shard on a plain deployment and N when sharded: one schema,
  either topology, and the single-node path exercises the exact SQL surface
  the sharded one needs. Sharding keys are **deterministic from row content**
  (`cityHash64(transaction_id)` for the fact tables, `cityHash64(ingester_id)`
  for the cursor and config series — now hashed with `cluster`), never `rand()`: at-least-once replay only
  deduplicates if the replayed row lands on the shard that already holds the
  original. The writer sets `insert_distributed_sync=1`, because an async
  distributed insert acks before any shard has the rows — which would break
  the write-then-cursor guarantee. Cross-table subqueries must use `GLOBAL IN`
  (double-distributed `IN` is denied); the Conflicts panel does.
- **Retention is bounded by TTLs in the schema files themselves** — history
  here is disposable by design, and disk growth must not depend on anyone
  remembering to prune: operations 30d, mutations 7d, conflict_ranges 7d,
  profiling_config 90d (the record of what the sample rate *was* outlives the
  data it scales).
- **Column codecs** are declared per column in the CREATEs: `Delta` on times,
  `DoubleDelta` on versionstamps, `Gorilla` on latencies, `ZSTD(3)` on keys
  and mutation values — the bulk of the bytes.
- `ingested_at` (`DEFAULT now()`) records when the daemon inserted the row;
  the difference to `event_time` is pipeline lag.
- **`sample_rate` carries the volume control that produced the row.** All three
  fact tables record the client sample rate the ingester observed when it wrote
  the row, so a sampled count can be scaled back to a cluster-wide estimate:

  ```sql
  round(sum(if(sample_rate > 0, 1.0 / sample_rate, 1.0)))
  ```

  `0` means unknown — profiling off, or no rate observed at ingest time — and
  the `if` is what keeps that zero out of the denominator rather
  than turning a hotspot table into a column of infinities. The dashboard uses
  exactly this expression, and an integration test pins the two together.

  It is approximate by construction: the stamp is the rate the daemon observed
  while *tailing*, not the rate the client applied when it made the sampling
  decision some seconds earlier. Over any window longer than the client flush
  interval the error is negligible; across an operator's retune, trust
  [`profiling_config`](#profiling_config) rather than these rows.

## `operations`

One row per profile event. The fact table every dashboard panel aggregates.

| Column | Type | Meaning |
| --- | --- | --- |
| `event_time` | `DateTime64(3)` | when the operation started, client clock |
| `cluster` | `LowCardinality(String)` | which FoundationDB cluster produced the row (`fdb.cluster_name`) |
| `op_type` | `LowCardinality(String)` | `get_version`, `get`, `get_range`, `commit`, `error_get`, `error_get_range`, `error_commit` |
| `key_prefix` | `LowCardinality(String)` | attribution — see below |
| `transaction_id` | `FixedString(16)` | joins events of one transaction |
| `event_seq` | `UInt32` | event's index within the record |
| `versionstamp` | `UInt64` | profile-write commit version, from the key |
| `dc_id` | `LowCardinality(String)` | client's datacenter locality; empty when unset |
| `tenant` | `String` | legacy tenant field; empty in practice |
| `debug_id` | `String` | client's `DEBUG_TRANSACTION_IDENTIFIER`, empty unless the app sets one |
| `latency` | `Float64` | seconds; `0` on error events, which carry none |
| `priority` | `LowCardinality(String)` | `default` / `batch` / `immediate` / `unset` |
| `read_version` | `Int64` | get_version only |
| `value_size` | `UInt32` | get only: size of the value read |
| `range_size` | `UInt32` | get_range only: bytes returned |
| `key` | `String` | get/error_get key; get_range/error_get_range begin |
| `end_key` | `String` | range end, range ops only |
| `num_mutations` | `UInt32` | commit only |
| `commit_bytes` | `UInt32` | commit only |
| `commit_version` | `Int64` | commit only |
| `read_snapshot_version` | `Int64` | commit / error_commit |
| `report_conflicting_keys` | `UInt8` | commit / error_commit flag |
| `lock_aware` | `UInt8` | commit / error_commit flag |
| `error_code` | `Int32` | error events; `0` otherwise (`1020` = lost a conflict) |
| `span_trace_id` | `String` | hex OTEL trace id when the commit carried a span |
| `span_id` | `UInt64` | OTEL span id |
| `sample_rate` | `Float64` | client sample rate in effect at ingest; `0` = unknown |

Engine and sort:

```sql
ENGINE = ReplacingMergeTree
PARTITION BY toDate(event_time)
ORDER BY (cluster, key_prefix, op_type, event_time, transaction_id, event_seq)
TTL toDateTime(event_time) + INTERVAL 30 DAY
```

The sort key leads with how the query analyzer groups — prefix, then operation,
then time — and ends with the unique event identity the replace-merge dedups on.

### How `key_prefix` is assigned

The configured prefixes (`prefixes:` in config.yaml) are matched
longest-match-first against the row's own key. Keys matching no configured
prefix are labelled with their own first 16 bytes (non-printable bytes
`\xNN`-escaped) plus ` (unknown)` — e.g. `/replication_log (unknown)` — so a
production dashboard points at where unclassified workload comes from instead
of hiding it all behind one anonymous row. Two synthetic buckets remain:
`(other)` for events carrying no key at all, and `(system)` for the `\xff`
keyspace. The distinct `(unknown)` labels are bounded by the distinct leading
16 bytes of unmatched keys, which keeps `LowCardinality(key_prefix)` honest
for real keyspaces.

Events that carry no key of their own — `get_version`, and `commit`, whose keys
live in its mutations — inherit the **transaction's** prefix: a vote over every
key the record touched (event keys and mutation keys), decided by class first
(configured prefix beats an `(unknown)` label beats `(other)` beats
`(system)`), then count, then lexicographically — never by map order, so
attribution is deterministic.

When `skip_system_keyspace` is on (default), a record whose vote lands on
`(system)` is dropped whole: that is FoundationDB's profiling machinery
profiling itself, and left in it tops every hotspot ranking on an idle cluster.

Beware the string-prefix semantics: a configured `/user` also matches
`/users/...`. Listing both `/user` and `/users` works (longest wins); a single
loose prefix silently absorbs its extensions.

**Every ingester writing to one database must share the same prefix
configuration.** Replayed rows dedup only because they are byte-identical; two
ingesters classifying the same record under different prefixes produce two
distinct rows, silently doubling every count.

## `mutations`

One row per mutation from a `commit` or `error_commit` event.

| Column | Type | Meaning |
| --- | --- | --- |
| `event_time` | `DateTime64(3)` | the parent commit's timestamp |
| `cluster` | `LowCardinality(String)` | which FoundationDB cluster produced the row |
| `key_prefix` | `LowCardinality(String)` | matched against this mutation's own key |
| `mutation_type` | `LowCardinality(String)` | `SET_VALUE`, `CLEAR_RANGE`, `ADD_VALUE`, `COMPARE_AND_CLEAR`, … |
| `transaction_id` | `FixedString(16)` | |
| `event_seq` | `UInt32` | which commit event within the record |
| `mutation_seq` | `UInt32` | index within the mutation list |
| `key` | `String` | param1: the written key, or a `CLEAR_RANGE` begin |
| `value` | `String` | param2 in full; **empty when `capture_mutation_values` is off** |
| `value_size` | `UInt32` | true `len(param2)`, materialised — real even when `value` is suppressed |
| `sample_rate` | `Float64` | client sample rate in effect at ingest; `0` = unknown |

```sql
ENGINE = ReplacingMergeTree
PARTITION BY toDate(event_time)
ORDER BY (cluster, key_prefix, key, event_time, transaction_id, event_seq, mutation_seq)
TTL toDateTime(event_time) + INTERVAL 7 DAY
```

Sorting by `(key_prefix, key, ...)` groups identical keys physically: `key`
compresses hard and the hot-write-key `GROUP BY key` reads sequentially.

Two things worth knowing:

- **Single-key clears are reconstructed.** FoundationDB serializes
  `tr.Clear(k)` on the wire as `(CLEAR_RANGE, keyAfter(k), "")`; the decoder
  reverses that, so the row shows `key = k` and `value = k + '\x00'` — the same
  pair FDB's own deserializer produces. (Upstream's Python analyzer does not
  reverse it and reports the wrong key.)
- **`value` is application payload.** With `capture_mutation_values: true`
  (the default, per the record-everything decision) ClickHouse holds a copy of
  everything the application writes, which may include personal data or
  secrets. `value_size` stays populated either way, so size analysis never
  needs the blob.

This is the bulkiest table, and it is separate from `operations` precisely so
it can carry its own retention — which it now does: **7 days**, applied by the
schema itself (`02_mutations.sql`), against 30 days on `operations`, 7 on
`conflict_ranges` and 90 on `profiling_config`. Retention is part of each
table's `CREATE`; retuning it is a schema change like any other — edit the
file, `make testenv_restart`.

## `conflict_ranges`

One row per read- or write-conflict range from a `commit` or `error_commit`,
written only when `capture_conflict_ranges` is on (default). Feeds the
dashboard's Conflicts row, which correlates it against `error_commit` rows to
answer "which ranges cause retries"; recorded in full because it cannot be
recovered later.

| Column | Type | Meaning |
| --- | --- | --- |
| `event_time` | `DateTime64(3)` | |
| `cluster` | `LowCardinality(String)` | which FoundationDB cluster produced the row |
| `key_prefix` | `LowCardinality(String)` | matched on `begin_key` |
| `kind` | `LowCardinality(String)` | `read` or `write` |
| `transaction_id` | `FixedString(16)` | |
| `event_seq` | `UInt32` | |
| `range_seq` | `UInt32` | index within its list |
| `begin_key`, `end_key` | `String` | half-open `[begin, end)` |
| `sample_rate` | `Float64` | client sample rate in effect at ingest; `0` = unknown |

```sql
ENGINE = ReplacingMergeTree
PARTITION BY toDate(event_time)
ORDER BY (cluster, key_prefix, kind, event_time, transaction_id, event_seq, range_seq)
TTL toDateTime(event_time) + INTERVAL 7 DAY
```

## `ingest_cursor`

The daemon's resume position — in ClickHouse so the daemon needs no volume.

| Column | Type | Meaning |
| --- | --- | --- |
| `cluster` | `LowCardinality(String)` | part of the identity: agents on different clusters may share a hostname-derived ingester id |
| `ingester_id` | `String` | one row per daemon identity (`-ingester-id`, default hostname) |
| `cursor_hex` | `String` | the safe profile key, hex-encoded (raw bytes include `\xff`) |
| `cursor_version` | `UInt64` | the cursor key's FDB commit versionstamp |
| `records_total` | `UInt64` | running delivered-record count for this ingester |
| `caught_up` | `UInt8` | whether the daemon had drained the keyspace when it saved this position; decides how a gap found on resume is worded |
| `updated_at` | `DateTime64(3)` | wall clock, informational only |

```sql
ENGINE = ReplacingMergeTree(cursor_version)
ORDER BY (cluster, ingester_id)
```

Two deliberate subtleties:

- **The stored cursor is the *safe* cursor, not the raw read position.** While
  a multi-chunk record is incomplete, the cursor deliberately lags to just
  before that record's first chunk, so a restart re-reads and redelivers it.
  The replay this causes is the at-least-once case the fact tables' replace
  keys absorb.
- **The replace version is the cursor's own versionstamp**, which is monotonic
  by construction. Versioning by wall clock would let a backwards NTP step or a
  host swap resurrect a stale cursor and replay hours as duplicates. Reads take
  the max `cursor_version`, so a skewed clock cannot move the daemon backwards.

## `profiling_config`

The cluster's client profiling settings, sampled over time — the two numbers
`fdbcli> profile client set <rate> <size>` controls, plus how full the resulting
ring buffer is.

`profile client set` is a point-in-time command with no history: ask
FoundationDB and you learn what the setting is *now*, never what it was during
the window you are looking at. Since the sample rate is the volume control on
every count in the fact tables, that makes those counts ambiguous on their own.
The daemon therefore records what it sees while tailing, which turns the
setting into a series the dashboard can plot beside the data it scales.

| Column | Type | Meaning |
| --- | --- | --- |
| `sampled_at` | `DateTime64(3)` | the **ingester's** clock at the moment of the read |
| `cluster` | `LowCardinality(String)` | whose setting this is — the sample rate is a per-cluster knob |
| `ingester_id` | `LowCardinality(String)` | which daemon saw it (`-ingester-id`, default hostname) |
| `enabled` | `UInt8` | whether sampling actually happens |
| `sample_rate` | `Float64` | fraction of transactions sampled, `0..1`; `0` when off or unset |
| `size_limit` | `UInt64` | ring-buffer cap in bytes; `0` when unset |
| `ring_buffer_bytes` | `UInt64` | bytes currently held in the profile keyspace |
| `ring_buffer_valid` | `UInt8` | `0` when the counter key is absent — nothing has ever been profiled |
| `laps` | `UInt64` | ring-buffer laps since the previous sample: the trim horizon passed the running daemon's cursor and unread records were deleted |
| `resume_gaps` | `UInt64` | the same discovered at startup — the loss happened while no daemon was running |
| `records_dropped` | `UInt64` | countable structural loss since the previous sample: records whose remaining chunks were trimmed or evicted mid-reassembly |
| `ingested_at` | `DateTime` | insert time |

```sql
ENGINE = ReplacingMergeTree
PARTITION BY toDate(sampled_at)
ORDER BY (cluster, ingester_id, sampled_at)
TTL toDateTime(sampled_at) + INTERVAL 90 DAY
```

Four things worth knowing:

- **`sampled_at`, not `event_time`.** Every other table's `event_time` is the
  *client's* clock and lags by up to 10s. This is observed directly by the
  daemon and is current, so it deliberately does not share the name.
- **Rows are observations, not the cluster's own log.** A gap means no daemon
  was watching, which is itself worth seeing — and is not the same as profiling
  having been off, which reads as `enabled = 0`.
- **A change is written immediately; an unchanged setting is reaffirmed on a
  timer** (`ingest.profiling_sample_interval`, default 15s). That is what
  distinguishes "still 1.0 for this whole window" from "nobody looked". Plot it
  with stepped interpolation: the value holds until an operator changes it.
- **`enabled` is not `sample_rate > 0`.** `profile client set` stores `+inf` to
  mean disabled, so a present key is not the same as "on". The flag is derived
  once, in `internal/fdbconfig`, rather than re-inferred per query.
- **Loss rows are events, not record counts.** `laps` and `resume_gaps` count
  occasions on which history was torn; how many records each hole held is
  unknowable, because the records were deleted before anything read them.
  Only `records_dropped` — partially-read records the tailer had to abandon —
  is a true count. The dashboard's history-gap annotations and its History
  loss stat are driven from exactly these columns, and a loss event is written
  immediately rather than at the next sampling tick, so the annotation lands
  at the moment the hole was seen.

Where the two profiling keys live, for anyone reading raw FDB:

```
\xff/globalConfig/k/config/fdb_client_info/client_txn_sample_rate   tuple(float64)
\xff/globalConfig/k/config/fdb_client_info/client_txn_size_limit    tuple(int64)
\xff\x02/fdbClientInfo/client_latency_counter/                       uint64 LE
```
