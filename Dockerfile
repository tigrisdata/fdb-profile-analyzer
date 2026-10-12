# The profiler's container image: the fdbprof daemon that tails a
# FoundationDB cluster's transaction profiles into ClickHouse.
#
# Multi-arch: the FoundationDB client .deb (libfdb_c + headers, which cgo
# needs) is selected to match TARGETARCH. Build via `make image`, which stamps
# the version from `git describe`; the release workflow builds the same image
# and pushes it to quay.io/tigrisdata/fdb-profile-analyzer, stamped with the tag:
#
#   docker run --rm fdb-profile-analyzer:<version> -version
#
# Configuration is env-first (FDB_CLUSTER_FILE_CONTENTS, FDB_CLUSTER_NAME,
# CLICKHOUSE_*, INGESTER_ID); mount a config.yaml and pass `-config` for the
# rest, exactly as the test bed's ingester service does.
ARG GO_VERSION=1.27.1
ARG FDB_VERSION=7.3.79

FROM golang:${GO_VERSION}-bookworm AS build
ARG FDB_VERSION
ARG TARGETARCH
ARG VERSION=dev
ARG GIT_HASH=

# Upstream names the arm64 package "aarch64", not "arm64".
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) fdbarch=amd64 ;; \
        arm64) fdbarch=aarch64 ;; \
        *) echo "unsupported architecture ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/fdb-clients.deb \
        "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${fdbarch}.deb"; \
    dpkg -i /tmp/fdb-clients.deb; \
    rm /tmp/fdb-clients.deb

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build \
    -ldflags "-X 'github.com/pboros/fdb-profile-analyzer/internal/build.Version=${VERSION}' \
              -X 'github.com/pboros/fdb-profile-analyzer/internal/build.Hash=${GIT_HASH}'" \
    -o /out/fdbprof ./cmd/fdbprof

FROM debian:bookworm-slim
ARG VERSION=dev
ARG GIT_HASH=
LABEL org.opencontainers.image.title="fdb-profile-analyzer" \
      org.opencontainers.image.description="Tails FoundationDB client transaction profiles into ClickHouse" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_HASH}" \
      org.opencontainers.image.source="https://github.com/pboros/fdb-profile-analyzer"

COPY --from=build /usr/lib/libfdb_c.so /usr/lib/
COPY --from=build /out/fdbprof /usr/local/bin/

# The daemon needs no privileges: it reads FoundationDB, writes ClickHouse,
# and materialises FDB_CLUSTER_FILE_CONTENTS under TMPDIR.
USER nobody
ENV TMPDIR=/tmp
ENTRYPOINT ["fdbprof"]
