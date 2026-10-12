# Builds the Go tools that need libfdb_c — the load generator and the daemon —
# into one small image. Multi-arch: the FoundationDB client .deb is selected to
# match TARGETARCH, so this builds natively on both Apple Silicon and x86.
ARG GO_VERSION=1.27.1
ARG FDB_VERSION=7.3.79

FROM golang:${GO_VERSION}-bookworm AS build
ARG FDB_VERSION
ARG TARGETARCH

# The clients package carries both libfdb_c.so and the C headers cgo compiles
# against. Upstream names the arm64 package "aarch64", not "arm64".
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
RUN CGO_ENABLED=1 go build -o /out/loadgen ./cmd/loadgen \
    && CGO_ENABLED=1 go build -o /out/fdbprof ./cmd/fdbprof \
    && CGO_ENABLED=1 go build -o /out/profdump ./cmd/profdump

FROM debian:bookworm-slim
COPY --from=build /usr/lib/libfdb_c.so /usr/lib/
COPY --from=build /out/loadgen /out/fdbprof /out/profdump /usr/local/bin/
ENTRYPOINT ["loadgen"]
