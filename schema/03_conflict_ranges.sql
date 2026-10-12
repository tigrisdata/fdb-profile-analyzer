-- One row per read or write conflict range, discriminated by `kind`.
--
-- Feeds the dashboard's Conflicts row: correlated against error_commit events
-- to answer "which ranges are causing retries". Recorded in full because it
-- cannot be recovered later.
CREATE TABLE IF NOT EXISTS conflict_ranges_local
(
    event_time     DateTime64(3) CODEC(Delta, ZSTD),
    -- Which FoundationDB cluster produced this row. Several clusters typically
    -- feed one ClickHouse; this is the label the agent is configured with
    -- (fdb.cluster_name — "test" on the bundled test bed) and the dashboard's
    -- first filter. Leads the sort key so one cluster's data stays physically
    -- together.
    cluster        LowCardinality(String),
    key_prefix     LowCardinality(String),
    kind           LowCardinality(String),   -- 'read' or 'write'

    transaction_id FixedString(16),
    event_seq      UInt32,
    range_seq      UInt32,

    begin_key      String CODEC(ZSTD(3)),
    end_key        String CODEC(ZSTD(3)),

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

    ingested_at    DateTime DEFAULT now()
)
-- ReplacingMergeTree for the same at-least-once replay reason as operations.
ENGINE = ReplacingMergeTree
PARTITION BY toDate(event_time)
ORDER BY (cluster, key_prefix, kind, event_time, transaction_id, event_seq, range_seq)
-- Retention: conflict analysis is an incident-time activity; a week of ranges
-- is plenty, and this is the highest row-count table per transaction.
TTL toDateTime(event_time) + INTERVAL 7 DAY;

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
CREATE TABLE IF NOT EXISTS conflict_ranges AS conflict_ranges_local
ENGINE = Distributed('fdb', currentDatabase(), 'conflict_ranges_local', cityHash64(transaction_id));
