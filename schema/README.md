# ClickHouse schema

`.sql` files here are applied in filename order by `make testenv_schema`, which
is re-runnable at any time. Each file is one `CREATE TABLE IF NOT EXISTS`
carrying the table's complete shape — columns, codecs, TTL — so re-applying is
a no-op.

There is deliberately no in-place migration: `IF NOT EXISTS` never reconciles
an existing table with a changed definition. After ANY schema change, recreate
the test bed (`make testenv_restart`) or drop the affected tables first. The
data is derived and disposable by design; a migration path would be complexity
spent preserving what a rebuild regenerates.

Note this is *not* wired to ClickHouse's `/docker-entrypoint-initdb.d`. That
runs only on the very first boot of an empty data volume, which is a trap while
iterating on a schema — you would edit the SQL, restart, and see nothing change.
`make testenv_schema` re-applies on demand instead.
