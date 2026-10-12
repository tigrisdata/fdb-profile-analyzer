#!/usr/bin/env bash
#
# Runs inside the fdb container. Idempotent: safe to re-run on every
# `make testenv_up`.
#
# A freshly started fdbserver has no database and no client profiling, so this
# does the two one-time steps:
#   1. `configure new single <engine>` to create the database
#   2. `profile client set <rate> <size>` to turn on transaction profiling
#
set -euo pipefail

ENGINE="${FDB_STORAGE_ENGINE:-redwood}"
RATE="${FDB_PROFILE_RATE:-1.0}"
SIZE="${FDB_PROFILE_SIZE:-100MB}"
ATTEMPTS="${INIT_ATTEMPTS:-60}"

available() {
    fdbcli --exec 'status minimal' --timeout 3 2>/dev/null | grep -q 'The database is available'
}

echo "==> Waiting for FoundationDB to become available (engine: ${ENGINE})"
ok=0
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    if available; then
        ok=1
        break
    fi
    # Harmless once the database exists: `configure new` refuses to touch an
    # existing database, so this doubles as "create it if it isn't there yet"
    # while fdbserver is still coming up.
    fdbcli --exec "configure new single ${ENGINE}" --timeout 10 >/dev/null 2>&1 || true
    sleep 1
done

if [ "${ok}" != "1" ]; then
    echo "!!! FoundationDB did not become available. Last status:" >&2
    fdbcli --exec status --timeout 10 >&2 || true
    exit 1
fi
echo "    database is available"

echo "==> Enabling client transaction profiling (rate=${RATE} size=${SIZE})"
fdbcli --exec "profile client set ${RATE} ${SIZE}" --timeout 20

# Confirm the setting is visible to a *fresh* client. `profile client set`
# writes to global config, which clients fetch asynchronously by RPC, so a read
# immediately afterwards legitimately still reports "default". Retry rather than
# print something misleading.
confirmed=0
for _ in $(seq 1 30); do
    if fdbcli --exec 'profile client get' --timeout 20 2>/dev/null | grep -qv 'rate is set to default'; then
        if fdbcli --exec 'profile client get' --timeout 20 2>/dev/null | grep -q 'rate is set to [0-9]'; then
            confirmed=1
            break
        fi
    fi
    sleep 1
done

if [ "${confirmed}" = "1" ]; then
    fdbcli --exec 'profile client get' --timeout 20
else
    echo "!!! profiling config did not become visible to a fresh client" >&2
    echo "    the daemon will have nothing to tail; check: make testenv_verify" >&2
    exit 1
fi

echo "==> FoundationDB ready"
