-- The tailer's position in the profile keyspace.
--
-- Kept in ClickHouse rather than a file so the daemon needs no persistent
-- volume. Written only after the rows it covers are safely in, which is what
-- makes delivery at-least-once: a crash between the two replays a batch rather
-- than losing it.
--
-- The replace-merge version is cursor_version — the cursor key's own FDB
-- commit versionstamp, monotonic by construction. Versioning by wall clock
-- would let a backwards NTP step or a host swap resurrect a stale cursor and
-- silently replay hours of data.
CREATE TABLE IF NOT EXISTS ingest_cursor_local
(
    -- The FoundationDB cluster this cursor tracks. Part of the identity, not
    -- decoration: two agents tailing different clusters may both default their
    -- ingester_id to the same hostname, and must not trample each other's
    -- position.
    cluster        LowCardinality(String),
    ingester_id    String,
    -- The last safe profile key, hex-encoded: it contains arbitrary bytes
    -- including \xff, so it is not stored raw.
    cursor_hex     String,
    cursor_version UInt64,
    records_total  UInt64,
    -- Whether the daemon had drained the keyspace when it saved this position.
    -- Read back on resume to word a ring-buffer gap honestly: a caught-up
    -- daemon's downtime gap MAY have lost records, a behind daemon's gap
    -- certainly did.
    caught_up      UInt8,
    updated_at     DateTime64(3)
)
ENGINE = ReplacingMergeTree(cursor_version)
ORDER BY (cluster, ingester_id);

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
CREATE TABLE IF NOT EXISTS ingest_cursor AS ingest_cursor_local
ENGINE = Distributed('fdb', currentDatabase(), 'ingest_cursor_local', cityHash64(cluster, ingester_id));
