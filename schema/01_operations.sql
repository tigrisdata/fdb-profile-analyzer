-- One row per profile event. This is the fact table the query-analyzer panels
-- aggregate, so it stays flat and wide: events from one transaction share
-- transaction_id, and transaction-level views are a GROUP BY away.
--
-- Re-runnable: every statement is IF NOT EXISTS.
CREATE TABLE IF NOT EXISTS operations_local
(
    -- The event's own start_timestamp, from the client's clock. This is the
    -- dashboard's time axis, and it is NOT the same ordering as `versionstamp`:
    -- clients batch profile writes and flush every 10s, so events arrive out of
    -- order relative to the keyspace.
    event_time            DateTime64(3) CODEC(Delta, ZSTD),
    -- Which FoundationDB cluster produced this row. Several clusters typically
    -- feed one ClickHouse; this is the label the agent is configured with
    -- (fdb.cluster_name — "test" on the bundled test bed) and the dashboard's
    -- first filter. Leads the sort key so one cluster's data stays physically
    -- together.
    cluster               LowCardinality(String),
    op_type               LowCardinality(String),
    key_prefix            LowCardinality(String),

    transaction_id        FixedString(16),
    -- Index of the event inside its record. With transaction_id this uniquely
    -- identifies a row, which is what lets ReplacingMergeTree collapse the
    -- replays that at-least-once delivery produces.
    event_seq             UInt32,
    -- Commit version of the transaction that WROTE the profile, from the key.
    -- This is the tailer's ordering, kept for correlating with cursor position.
    versionstamp          UInt64 CODEC(DoubleDelta, ZSTD),

    dc_id                 LowCardinality(String),
    tenant                String,
    -- The client's DEBUG_TRANSACTION_IDENTIFIER, empty unless the application
    -- sets one.
    debug_id              String,

    latency               Float64 CODEC(Gorilla, ZSTD),   -- seconds; 0 on error events

    priority              LowCardinality(String),
    read_version          Int64,

    value_size            UInt32,
    range_size            UInt32,

    key                   String CODEC(ZSTD(3)),
    end_key               String CODEC(ZSTD(3)),

    num_mutations         UInt32,
    commit_bytes          UInt32,
    commit_version        Int64,
    read_snapshot_version Int64,
    report_conflicting_keys UInt8,
    lock_aware            UInt8,

    error_code            Int32,

    span_trace_id         String,
    span_id               UInt64,

    -- The client sample rate in effect when this row was ingested. Stored per
    -- row so counts can be extrapolated back to cluster-wide totals: at 0.01
    -- each row stands for about a hundred transactions. 0 means unknown or
    -- profiling off, which the dashboard reads as "do not scale".
    --
    -- Approximate by construction. It is the rate the daemon observed while
    -- tailing, not the rate the client applied when it made the sampling
    -- decision some seconds earlier, so rows written either side of an
    -- operator's retune can carry the neighbouring value. Over any window
    -- longer than the client flush interval the error is negligible; across a
    -- retune boundary, trust the profiling_config series, not these rows.
    sample_rate           Float64,

    ingested_at           DateTime DEFAULT now()
)
-- ReplacingMergeTree because delivery is at-least-once: a crash between the
-- insert and the cursor save replays a batch. Replayed rows are identical in
-- every sort-key column, so background merges collapse them. Dedup is eventual
-- (exact counts would need FINAL); a transient double-count after a crash
-- beats a permanent one.
ENGINE = ReplacingMergeTree
-- Time ranges prune whole partitions.
PARTITION BY toDate(event_time)
-- Matches how the main table groups: prefix, then operation, then time. The
-- trailing (transaction_id, event_seq) makes the key unique per logical row,
-- which is what the replace-merge dedups on.
ORDER BY (cluster, key_prefix, op_type, event_time, transaction_id, event_seq)
-- Retention. History here is disposable by design — the pipeline sheds it
-- rather than pressuring the cluster, and the same stance applies to disk:
-- growth must be bounded without anyone remembering to prune. Thirty days of
-- events is the analysis window; the bulky payload tables keep less (02/03).
-- Retuning it is a schema change like any other: edit, then testenv_restart.
TTL toDateTime(event_time) + INTERVAL 30 DAY;

-- The name everything above the schema uses — the writer's INSERTs, every
-- dashboard panel, the tests. A Distributed table over the `fdb` cluster,
-- which has one shard on the plain test bed and two under SHARDED=1: one
-- schema, either topology.
--
-- The sharding key must be DETERMINISTIC from row content, never rand():
-- delivery is at-least-once, and a replayed row only collapses in the
-- ReplacingMergeTree if it lands on the same shard as the original. rand()
-- would pass every single-node test and silently double-count after the first
-- daemon crash in production. cityHash64(transaction_id) also co-locates one
-- transaction's rows across operations, mutations and conflict_ranges, which
-- keeps per-transaction analysis shard-local.
CREATE TABLE IF NOT EXISTS operations AS operations_local
ENGINE = Distributed('fdb', currentDatabase(), 'operations_local', cityHash64(transaction_id));
