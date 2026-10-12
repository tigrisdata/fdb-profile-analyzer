-- The cluster's client profiling settings, sampled over time.
--
-- `fdbcli> profile client set <rate> <size>` is a point-in-time command with no
-- history: ask FoundationDB and you learn what the setting is now, never what
-- it was during the window you are looking at. That makes every count on the
-- dashboard ambiguous, because the sample rate is the volume control on all of
-- them. The daemon therefore records what it sees while tailing, turning the
-- setting into a time series the dashboard can plot beside the data it scales.
--
-- Rows are the ingester's observations, not the cluster's own log, so a gap
-- means "no daemon was watching", which is itself worth seeing.
--
-- Re-runnable: every statement is IF NOT EXISTS.
CREATE TABLE IF NOT EXISTS profiling_config_local
(
    -- The ingester's clock at the moment of the read. Deliberately NOT named
    -- event_time: every other table's event_time is the *client's* clock and
    -- lags by up to 10s, whereas this is observed directly and is current.
    sampled_at        DateTime64(3),
    -- Which FoundationDB cluster the setting belongs to: the sample rate is a
    -- per-cluster knob, and with several clusters feeding one ClickHouse an
    -- unlabelled rate series would be meaningless.
    cluster           LowCardinality(String),
    -- Which daemon saw it. Several ingesters watching one cluster should agree;
    -- when they do not, that disagreement is the finding.
    ingester_id       LowCardinality(String),

    -- Whether sampling actually happens. `profile client set` stores +inf to
    -- mean disabled, so a present key is not the same as "on" — hence a flag
    -- rather than inferring it from sample_rate.
    enabled           UInt8,
    -- Fraction of transactions sampled, 0..1. Stored as 0 when profiling is
    -- off or was never configured, which is what keeps it out of the
    -- extrapolation arithmetic instead of scaling counts by infinity.
    sample_rate       Float64,
    -- Cap in bytes on the profile keyspace ring buffer: the `100MB` half of
    -- `profile client set 1.0 100MB`. 0 when unset.
    size_limit        UInt64,

    -- How much of that cap is in use, from the profile keyspace's own running
    -- byte counter. When this approaches size_limit FoundationDB is trimming
    -- oldest-first, potentially past a lagging daemon's cursor — the one
    -- failure mode that loses data silently.
    ring_buffer_bytes UInt64,
    -- The counter is absent until something has been profiled, and the read is
    -- best effort inside the poll transaction, so 0 is ambiguous without this.
    ring_buffer_valid UInt8,

    -- History loss since the previous sample. The pipeline sheds profile
    -- history rather than ever pressuring the cluster; these columns are what
    -- keeps that shedding visible. laps: the ring buffer's trim horizon passed
    -- the running daemon's cursor — records were deleted unread. resume_gaps:
    -- the same discovered at startup, where the loss happened while nothing
    -- was watching. Both count EVENTS: a lap's size in records is unknowable,
    -- and pretending otherwise would be fabricating data. records_dropped is
    -- the countable structural loss — records whose remaining chunks were
    -- trimmed or evicted mid-reassembly.
    laps              UInt64,
    resume_gaps       UInt64,
    records_dropped   UInt64,

    ingested_at       DateTime DEFAULT now()
)
-- Same at-least-once reasoning as the fact tables, and additionally: the daemon
-- re-records the setting whenever it changes, so an unchanged setting sampled
-- twice inside the same millisecond collapses rather than fattening the series.
ENGINE = ReplacingMergeTree
PARTITION BY toDate(sampled_at)
ORDER BY (cluster, ingester_id, sampled_at)
-- Retention: a few KB per day, and it is the record of what the sample rate
-- WAS — the one thing that makes old counts interpretable — so it outlives
-- the fact tables.
TTL toDateTime(sampled_at) + INTERVAL 90 DAY;

-- The name everything above the schema uses — the writer's INSERTs, every
-- dashboard panel, the tests. A Distributed table over the `fdb` cluster,
-- which has one shard on the plain test bed and two under SHARDED=1: one
-- schema, either topology.
--
-- The sharding key must be DETERMINISTIC from row content, never rand():
-- delivery is at-least-once, and a replayed row only collapses in the
-- ReplacingMergeTree if it lands on the same shard as the original. rand()
-- would pass every single-node test and silently double-count after the first
-- daemon crash in production. Hashing ingester_id keeps one ingester's rows
-- on one shard, so the replace-merge and the max-version read both stay
-- shard-local.
CREATE TABLE IF NOT EXISTS profiling_config AS profiling_config_local
ENGINE = Distributed('fdb', currentDatabase(), 'profiling_config_local', cityHash64(cluster, ingester_id));
