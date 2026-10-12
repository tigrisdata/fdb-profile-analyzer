# Test bed for the FoundationDB profile -> ClickHouse -> Grafana pipeline.
#
#   make testenv_up     bring the environment up and make it ready to use
#   make testenv_down   tear it down, including data volumes
#
# `make help` lists everything.

SHELL := /bin/bash

# .env holds ports, versions and credentials for the test bed.
ifneq (,$(wildcard .env))
include .env
export
endif

# Compose v2 plugin if available, else the standalone binary.
COMPOSE_BIN ?= $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; else echo "docker-compose"; fi)
export COMPOSE_BIN

# SHARDED=1 overlays a second ClickHouse shard. The schema is identical either
# way (Distributed tables always exist); this flag only decides how many nodes
# the `fdb` cluster has. Teardown always uses the full file set so a sharded
# bed is removed cleanly whatever flags the down was invoked with.
SHARDED ?= 0
ifeq ($(SHARDED),1)
COMPOSE := $(COMPOSE_BIN) -f docker-compose.yml -f docker-compose.sharded.yml
CH_NODES := clickhouse clickhouse2
else
COMPOSE := $(COMPOSE_BIN)
CH_NODES := clickhouse
endif
COMPOSE_ALL := $(COMPOSE_BIN) -f docker-compose.yml -f docker-compose.sharded.yml

# The identity stamped into binaries and images: the release tag when the
# release workflow passes one (make dist VERSION=vX.Y.Z), otherwise what
# `git describe` says — the nearest vX.Y.Z tag plus the commit hash, or just the
# hash in a clone without the tags.
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GIT_HASH ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "")
LDFLAGS_X   := -X github.com/pboros/fdb-profile-analyzer/internal/build.Version=$(VERSION) -X github.com/pboros/fdb-profile-analyzer/internal/build.Hash=$(GIT_HASH)
BUILD_PARAM := -ldflags "$(LDFLAGS_X)"

# The Go container `make dist` builds in runs the Go version go.mod pins, and
# keeps a module and build cache between runs.
GO_VERSION := $(shell sed -n 's/^go //p' go.mod)
GOCACHES   := -v fdb-profile-analyzer-gomod:/go/pkg/mod -v fdb-profile-analyzer-gocache:/root/.cache/go-build
# The build there runs with -buildvcs=false: on a Linux docker host (CI) the tree is owned by
# the host user while the container runs as root, and git refuses a repository owned by someone
# else ("dubious ownership", exit 128), which Go's VCS stamping turns into a failed build.
# Nothing reads the stamp; the version and hash are the -X flags in LDFLAGS_X.
GOBUILDENV := -e GOFLAGS=-buildvcs=false

TESTENV      := go run ./cmd/testenv
SCHEMA_DIR   := schema
CLUSTER_DIR  := .testenv
CLUSTER_FILE := $(CLUSTER_DIR)/fdb.cluster

FDB_VERSION            ?= 7.3.79
FDB_PORT               ?= 4500
FDB_CLUSTER_NAME       ?= test
CLICKHOUSE_HTTP_PORT   ?= 8123
CLICKHOUSE_NATIVE_PORT ?= 9000
CLICKHOUSE2_NATIVE_PORT ?= 9001
CLICKHOUSE_DB          ?= fdb_profiles
CLICKHOUSE_USER        ?= fdb
CLICKHOUSE_PASSWORD    ?= fdb
GRAFANA_PORT           ?= 3000
GRAFANA_ADMIN_USER     ?= admin
GRAFANA_ADMIN_PASSWORD ?= admin

CH_CLIENT = $(COMPOSE) exec -T clickhouse clickhouse-client --host 127.0.0.1 \
	--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD)

# Which service `testenv_logs` follows; override with SVC=clickhouse.
SVC  ?=
# Which load profile `testenv_load` runs: basic | big | conflict | hotspot | all.
LOAD ?= all
# Extra flags passed through to cmd/loadgen, e.g. LOADGEN_FLAGS="-prefixes /a,/b".
LOADGEN_FLAGS ?=
# Daemon configuration file.
CONFIG ?= config.yaml
DAEMON_FLAGS ?=
# Where integration results are recorded.
RESULTS_DIR ?= test-results
INTEGRATION_TIMEOUT ?= 20m

.DEFAULT_GOAL := help

.PHONY: help test testenv_up testenv_down testenv_restart testenv_status testenv_logs \
        testenv_verify testenv_schema testenv_load testenv_profile_stats \
        testenv_smoke testenv_fdbcli testenv_ch testenv_pull testenv_dump \
        run ingest_once demo load_start load_stop load_logs integration_test require_testenv cluster_file deps_check \
        integration_test_sharded integration_test_all build image dist hooks

help: ## Show this help
	@printf 'Targets:\n'
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@printf '\nVariables: SVC=<service>  LOAD=basic|big|conflict|all\n'

# --- lifecycle --------------------------------------------------------------

testenv_up: ## Bring up FoundationDB + ClickHouse + Grafana and make them ready
	@echo "==> Starting containers"
	$(COMPOSE) up -d
	@$(MAKE) --no-print-directory cluster_file
	@echo "==> Configuring FoundationDB and enabling profiling"
	@for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T fdb bash /testenv/init.sh; then break; fi; \
		sleep 2; \
	done
	@echo "==> Waiting for the stack to become usable"
	@$(TESTENV) wait
	@$(MAKE) --no-print-directory testenv_schema
	@echo ""
	@echo "Test environment ready."
	@echo "  FoundationDB  127.0.0.1:$(FDB_PORT)   cluster file: $(CLUSTER_FILE)"
	@echo "  ClickHouse    http://localhost:$(CLICKHOUSE_HTTP_PORT)   native :$(CLICKHOUSE_NATIVE_PORT)   db=$(CLICKHOUSE_DB) user=$(CLICKHOUSE_USER)"
	@echo "  Grafana       http://localhost:$(GRAFANA_PORT)   (anonymous admin; login $(GRAFANA_ADMIN_USER)/$(GRAFANA_ADMIN_PASSWORD))"
	@echo ""
	@echo "  export FDB_CLUSTER_FILE=$(CURDIR)/$(CLUSTER_FILE)"
	@echo ""
	@echo "Next:  make testenv_verify   then   make testenv_smoke"

testenv_down: ## Tear down the environment and delete its data volumes
	@echo "==> Stopping containers and removing volumes"
	$(COMPOSE_ALL) --profile load down --volumes --remove-orphans
	@rm -rf $(CLUSTER_DIR)
	@echo "Test environment removed."

testenv_restart: ## Recreate the environment from scratch
	@$(MAKE) --no-print-directory testenv_down
	@$(MAKE) --no-print-directory testenv_up

testenv_pull: ## Pull the pinned images
	$(COMPOSE) pull

# --- inspection -------------------------------------------------------------

testenv_status: ## Show container status and FoundationDB status
	$(COMPOSE) ps
	@echo ""
	-$(COMPOSE) exec -T fdb fdbcli --exec status --timeout 15

testenv_logs: ## Follow logs (SVC=<service> for just one)
	$(COMPOSE) logs -f --tail 100 $(SVC)

testenv_verify: require_testenv ## Check all three services are usable, incl. Grafana -> ClickHouse
	@$(TESTENV) verify

testenv_fdbcli: ## Open an interactive fdbcli against the test cluster
	$(COMPOSE) exec fdb fdbcli

testenv_ch: ## Open an interactive clickhouse-client
	$(COMPOSE) exec clickhouse clickhouse-client --host 127.0.0.1 \
		--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD) --database $(CLICKHOUSE_DB)

# --- data -------------------------------------------------------------------

testenv_schema: require_testenv ## (Re-)apply schema/*.sql
	@if ! ls $(SCHEMA_DIR)/*.sql >/dev/null 2>&1; then \
		echo "==> No schema files yet ($(SCHEMA_DIR)/*.sql)"; \
	else \
		for node in $(CH_NODES); do \
			for f in $(SCHEMA_DIR)/*.sql; do \
				echo "==> Applying $$f on $$node"; \
				$(COMPOSE) exec -T $$node clickhouse-client --host 127.0.0.1 \
					--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD) \
					--database $(CLICKHOUSE_DB) --multiquery < "$$f" || exit 1; \
			done; \
		done; \
	fi

# Fails with an actionable message when the test bed is not running, rather than
# letting FoundationDB report "no cluster file found" from a path that
# `testenv_down` deleted.
require_testenv:
	@if [ ! -f $(CLUSTER_FILE) ]; then \
		echo ""; \
		echo "The test environment is not running: $(CLUSTER_FILE) is missing."; \
		echo "It is removed by 'make testenv_down' and by 'make integration_test'."; \
		echo ""; \
		echo "  Start it with:  make testenv_up"; \
		echo ""; \
		exit 1; \
	fi
	@if ! $(COMPOSE) ps --status running --services 2>/dev/null | grep -q '^fdb$$'; then \
		echo ""; \
		echo "The fdb container is not running."; \
		echo ""; \
		echo "  Start it with:  make testenv_up"; \
		echo ""; \
		exit 1; \
	fi

testenv_load: require_testenv ## Generate profiled transactions (LOAD=basic|big|conflict|hotspot|all)
	@echo "==> Go load generator (needs the host FDB client; see make deps_check)"
	FDB_CLUSTER_FILE=$(CURDIR)/$(CLUSTER_FILE) go run ./cmd/loadgen -mode $(LOAD) $(LOADGEN_FLAGS)

testenv_profile_stats: require_testenv ## Report what is sitting in the FDB profile keyspace
	@$(TESTENV) stats

testenv_dump: require_testenv ## Tail and decode profile records, with a hotspot ranking
	FDB_CLUSTER_FILE=$(CURDIR)/$(CLUSTER_FILE) go run ./cmd/profdump $(DUMP_FLAGS)

run: require_testenv ## Run the ingest daemon against the test bed
	FDB_CLUSTER_FILE=$(CURDIR)/$(CLUSTER_FILE) go run ./cmd/fdbprof -config $(CONFIG) $(DAEMON_FLAGS)

ingest_once: require_testenv ## Drain everything currently in the profile keyspace into ClickHouse, then exit
	FDB_CLUSTER_FILE=$(CURDIR)/$(CLUSTER_FILE) go run ./cmd/fdbprof -config $(CONFIG) -once

testenv_smoke: ## End-to-end: load -> FoundationDB -> daemon -> ClickHouse
	@$(MAKE) --no-print-directory testenv_load
	@$(MAKE) --no-print-directory ingest_once
	@$(MAKE) --no-print-directory testenv_dump

load_start: require_testenv ## Start containers for continuous load AND its ingestion
	$(COMPOSE) --profile load up -d --build loadgen ingester
	@echo ""
	@echo "Continuous load and ingestion running — the dashboard updates on its own."
	@echo "Watch them:   make load_logs"
	@echo "Stop them:    make load_stop"

load_stop: ## Stop and remove the load and ingester containers
	$(COMPOSE) --profile load rm -sf loadgen ingester

load_logs: ## Follow the load and ingester containers' logs
	$(COMPOSE) --profile load logs -f loadgen ingester

demo: ## Bring everything up, generate load, ingest it, and leave it running for the dashboard
	@$(MAKE) --no-print-directory testenv_up
	@echo ""
	@$(MAKE) --no-print-directory testenv_load
	@echo ""
	@$(MAKE) --no-print-directory ingest_once
	@echo ""
	@echo "Dashboard: http://localhost:$(GRAFANA_PORT)/d/fdb-profile-analyzer"
	@echo "Containers are still running. Tear down with: make testenv_down"

# The env var block every test invocation needs. CLICKHOUSE2_ADDRESS is set
# only under SHARDED=1: its presence is what arms the topology test.
ifeq ($(SHARDED),1)
ITEST_ENV = FDB_CLUSTER_FILE=$(CURDIR)/$(CLUSTER_FILE) \
	FDB_CLUSTER_NAME=$(FDB_CLUSTER_NAME) \
	CLICKHOUSE_ADDRESS=localhost:$(CLICKHOUSE_NATIVE_PORT) \
	CLICKHOUSE2_ADDRESS=localhost:$(CLICKHOUSE2_NATIVE_PORT) \
	CLICKHOUSE_DB=$(CLICKHOUSE_DB) \
	CLICKHOUSE_USER=$(CLICKHOUSE_USER) \
	CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD)
RESULT_NAME ?= integration_sharded
else
ITEST_ENV = FDB_CLUSTER_FILE=$(CURDIR)/$(CLUSTER_FILE) \
	FDB_CLUSTER_NAME=$(FDB_CLUSTER_NAME) \
	CLICKHOUSE_ADDRESS=localhost:$(CLICKHOUSE_NATIVE_PORT) \
	CLICKHOUSE_DB=$(CLICKHOUSE_DB) \
	CLICKHOUSE_USER=$(CLICKHOUSE_USER) \
	CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD)
RESULT_NAME ?= integration
endif

integration_test: ## Full run: clean slate, run integration tests, record results, tear down
	@mkdir -p $(RESULTS_DIR) && rm -f $(RESULTS_DIR)/$(RESULT_NAME).* $(RESULTS_DIR)/$(RESULT_NAME)-lap.log $(RESULTS_DIR)/setup.log
	@echo "==> Ensuring a clean slate (removes any running test environment)"
	@$(MAKE) --no-print-directory testenv_down > /dev/null 2>&1 || true
	@echo "==> Starting the test environment"
	@$(MAKE) --no-print-directory testenv_up > $(RESULTS_DIR)/setup.log 2>&1 \
		|| { echo "setup failed; see $(RESULTS_DIR)/setup.log"; tail -20 $(RESULTS_DIR)/setup.log; \
		     $(MAKE) --no-print-directory testenv_down > /dev/null 2>&1; exit 1; }
	@echo "==> Running integration tests"
	@set -o pipefail; \
	rc=0; \
	$(ITEST_ENV) \
		go test -tags=integration -count=1 -v -timeout $(INTEGRATION_TIMEOUT) -json ./test/... \
		| tee $(RESULTS_DIR)/$(RESULT_NAME).jsonl \
		| $(TESTENV) report -in - -out $(RESULTS_DIR) -name $(RESULT_NAME) || rc=$$?; \
	if [ $$rc -eq 0 ]; then \
		echo "==> Ring-buffer lap test (shrinks the profile buffer, so it runs last, alone)"; \
		$(ITEST_ENV) \
		LAP_TEST=1 \
			go test -tags=integration -count=1 -v -timeout 10m -run TestRingBufferLapIsVisible ./test/integration/ \
			> $(RESULTS_DIR)/$(RESULT_NAME)-lap.log 2>&1 \
			&& grep -E -- "--- (PASS|SKIP)" $(RESULTS_DIR)/$(RESULT_NAME)-lap.log \
			|| { rc=$$?; echo "lap test failed; see $(RESULTS_DIR)/$(RESULT_NAME)-lap.log"; tail -40 $(RESULTS_DIR)/$(RESULT_NAME)-lap.log; }; \
	fi; \
	echo "==> Tearing down the test environment"; \
	$(MAKE) --no-print-directory testenv_down >> $(RESULTS_DIR)/setup.log 2>&1 || true; \
	exit $$rc

integration_test_sharded: ## integration_test against a two-shard ClickHouse (SHARDED=1)
	@# RESULT_NAME on the command line, not just via ifeq: the Makefile's bare
	@# `export` puts the parent's value in the sub-make's environment, where a
	@# ?= default cannot override it. Command-line origin outranks both.
	@$(MAKE) --no-print-directory SHARDED=1 RESULT_NAME=integration_sharded integration_test

integration_test_all: ## Both topologies back to back: single-shard, then sharded
	@$(MAKE) --no-print-directory integration_test
	@$(MAKE) --no-print-directory integration_test_sharded

build: ## Build the binaries into bin/, version stamped from git describe
	go build $(BUILD_PARAM) -o bin/ ./cmd/...

# The release workflow builds this same Dockerfile and pushes it to
# quay.io/tigrisdata/fdb-profile-analyzer, stamped with the tag (README, "CI and releases").
image: ## Build the profiler container image, tagged with the current version
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_HASH=$(GIT_HASH) \
		--build-arg FDB_VERSION=$(FDB_VERSION) \
		-t fdb-profile-analyzer:$(VERSION) -t fdb-profile-analyzer:latest .

dist: ## Release binary (linux/amd64, cgo against FDB_VERSION's client) and checksums in dist/, VERSION stamped in (make dist VERSION=v1.2.3)
	rm -rf dist && mkdir -p dist
	docker run --rm --platform linux/amd64 -v "$(CURDIR)":/src -w /src $(GOCACHES) \
		$(GOBUILDENV) -e CGO_ENABLED=1 -e GOOS=linux -e GOARCH=amd64 golang:$(GO_VERSION)-bookworm sh -c '\
		set -eu; \
		curl -fsSL -o /tmp/fdb-clients.deb "https://github.com/apple/foundationdb/releases/download/$(FDB_VERSION)/foundationdb-clients_$(FDB_VERSION)-1_amd64.deb"; \
		dpkg -i /tmp/fdb-clients.deb >/dev/null; \
		go build -trimpath -ldflags "-s -w $(LDFLAGS_X)" -o dist/fdbprof-linux-amd64 ./cmd/fdbprof; \
		cd dist && sha256sum fdbprof-linux-amd64 > checksums.txt'
	@ls -l dist

hooks: ## Install the repository's git hooks (a commit-msg check for the version-bump prefix)
	git config core.hooksPath .githooks
	@echo "git hooks installed from .githooks (commit subjects must start with major:, minor: or patch:)"

test: ## Build, vet (the integration suite too) and run the unit tests
	go build ./... && go vet ./... && go vet -tags=integration ./test/... && go test ./...

# --- host tooling -----------------------------------------------------------

cluster_file: ## Write the host-side FDB cluster file
	@mkdir -p $(CLUSTER_DIR)
	@echo "docker:docker@127.0.0.1:$(FDB_PORT)" > $(CLUSTER_FILE)
	@echo "==> Wrote $(CLUSTER_FILE)"

deps_check: ## Check for the host FDB client library that Go tests need
	@echo "==> Host FoundationDB client"
	@if command -v fdbcli >/dev/null 2>&1; then \
		echo "    ok    fdbcli: $$(command -v fdbcli) ($$(fdbcli --version 2>/dev/null | head -1))"; \
	else \
		echo "    FAIL  fdbcli not found on PATH"; \
	fi
	@lib=$$(ls /usr/local/lib/libfdb_c.* /usr/lib/libfdb_c.* /opt/homebrew/lib/libfdb_c.* 2>/dev/null | head -1); \
	if [ -n "$$lib" ]; then \
		echo "    ok    libfdb_c: $$lib"; \
	else \
		echo "    FAIL  libfdb_c not found"; \
	fi
	@echo ""
	@echo "    The Go bindings link against libfdb_c, so host-run tests need the"
	@echo "    client package. The containers do not need it."
	@echo "    macOS: https://github.com/apple/foundationdb/releases/download/$(FDB_VERSION)/FoundationDB-$(FDB_VERSION)_$$(uname -m).pkg"
	@echo "    Linux: foundationdb-clients_$(FDB_VERSION)-1_amd64.deb from the same release"
