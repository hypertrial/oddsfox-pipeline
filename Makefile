.PHONY: ci-fast ci-fast-core ci-fast-goal ci-fast-static-docs ci-fast-tests ci-fast-dbt release-gate release-gate-core release-gate-goal release-gate-coverage release-gate-coverage-prep release-gate-cov-unit release-gate-cov-unit-run release-gate-cov-dagster-jobs release-gate-cov-dagster-jobs-run release-gate-cov-dagster-refresh release-gate-cov-dagster-refresh-run release-gate-cov-dbt-incremental release-gate-cov-dbt-incremental-run release-gate-cov-dbt-serial release-gate-cov-dbt-serial-run coverage-combine-report coverage-combine-report-run release-gate-dbt-quality release-gate-dbt-unit release-gate-dbt-freshness release-gate-dbt-polygon release-gate-dbt-match-order-book release-gate-dbt-match-minute release-gate-dbt-minute-odds release-gate-dbt-market-portrait release-gate-dbt-build release-gate-costguard-scan release-gate-mutation release-gate-static-docs release-gate-static-checks release-gate-python-lint release-gate-dbt-lint release-gate-docs-build release-gate-docs-test package-smoke runtime-dirs local-marts-rebuild match-minute-inputs-validate minute-odds-backfill minute-odds-snapshot-rebuild stage-minute-input-release minute-odds-live-smoke futures-minute-publish-benchmark dagster-dev dagster-jobs-smoke dagster-jobs-smoke-cov dagster-refresh-cov dbt-build dbt-build-ci dbt-lint dbt-prepare dbt-polygon-settlement-ci dbt-match-order-book-ci dbt-match-minute-ci dbt-minute-odds-ci dbt-market-portrait-ci market-portrait-target-validate dbt-parse dbt-test dbt-unit dbt-source-freshness-ci golden-dbt data-quality mutation mutation-ci contract-http match-minute-live-smoke match-order-book-live-smoke market-portrait-live-backfill polygon-runtime-dirs polygon-settlement-benchmark polygon-settlement-export polygon-settlement-live-smoke polygon-settlement-release polygon-settlement-seed-candidate polygon-settlement-seed-validate export-wc2026-elo-freezes costguard costguard-scan docs-serve docs-build docs-test docs-check clean-local-artifacts format lint python-lint test test-dev test-cov coverage coverage-erase coverage-report unit-core unit-ingest unit-orchestration integration-dbt integration-dbt-parallel integration-dbt-serial integration-dbt-cov integration-dbt-cov-parallel integration-dbt-cov-serial integration-dagster integration-dagster-cov pipelines-deterministic check-repository check-distribution check-secrets check-terminology compact-warehouse prune-odds-history gate-timing

.PHONY: dbt-soccer-minute-ci soccer-catalog-audit soccer-minute-live-smoke soccer-minute-backfill soccer-production-health soccer-minute-performance-benchmark

REPO_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
override PYTHON := $(shell if test -x "$(REPO_ROOT)/.venv/bin/python"; then printf '%s' "$(REPO_ROOT)/.venv/bin/python"; else printf 'python3'; fi)
COSTGUARD ?= costguard
ODDSFOX_STORAGE_ROOT ?= $(REPO_ROOT)
ODDSFOX_RUNTIME_ROOT ?= $(REPO_ROOT)/.cache/runtime
ODDSFOX_RUNTIME_TMP := $(ODDSFOX_RUNTIME_ROOT)/tmp
ODDSFOX_RUNTIME_XDG := $(ODDSFOX_RUNTIME_ROOT)/xdg
ODDSFOX_RUNTIME_PYCACHE := $(ODDSFOX_RUNTIME_ROOT)/pycache
ODDSFOX_RUNTIME_DBT_TARGET := $(ODDSFOX_RUNTIME_ROOT)/dbt-target
ODDSFOX_RUNTIME_DBT_LOGS := $(ODDSFOX_RUNTIME_ROOT)/dbt-logs
ODDSFOX_RUNTIME_DUCKDB_EXTENSIONS := $(ODDSFOX_RUNTIME_ROOT)/duckdb-extensions
# Shared across parallel gate lanes (install/cache once under the default runtime root).
ODDSFOX_RUNTIME_UV := $(REPO_ROOT)/.cache/runtime/uv
ODDSFOX_RUNTIME_UV_PYTHON := $(REPO_ROOT)/.cache/runtime/uv-python
ODDSFOX_RUNTIME_PLAYWRIGHT := $(REPO_ROOT)/.cache/runtime/ms-playwright
DAGSTER_DEV_TMP := /tmp/oddsfox-dg-$(shell id -u)
export TMPDIR := $(ODDSFOX_RUNTIME_TMP)
export TMP := $(ODDSFOX_RUNTIME_TMP)
export TEMP := $(ODDSFOX_RUNTIME_TMP)
export XDG_CACHE_HOME := $(ODDSFOX_RUNTIME_XDG)
export UV_CACHE_DIR := $(ODDSFOX_RUNTIME_UV)
export UV_PYTHON_INSTALL_DIR := $(ODDSFOX_RUNTIME_UV_PYTHON)
export PYTHONPYCACHEPREFIX := $(ODDSFOX_RUNTIME_PYCACHE)
export DBT_TARGET_PATH := $(ODDSFOX_RUNTIME_DBT_TARGET)
export DBT_LOG_PATH := $(ODDSFOX_RUNTIME_DBT_LOGS)
export PLAYWRIGHT_BROWSERS_PATH := $(ODDSFOX_RUNTIME_PLAYWRIGHT)
ODDSFOX_RUNTIME_DIRS := "$(ODDSFOX_RUNTIME_TMP)" "$(ODDSFOX_RUNTIME_XDG)" "$(ODDSFOX_RUNTIME_UV)" "$(ODDSFOX_RUNTIME_UV_PYTHON)" "$(ODDSFOX_RUNTIME_PYCACHE)" "$(ODDSFOX_RUNTIME_DBT_TARGET)" "$(ODDSFOX_RUNTIME_DBT_LOGS)" "$(ODDSFOX_RUNTIME_DUCKDB_EXTENSIONS)" "$(ODDSFOX_RUNTIME_PLAYWRIGHT)"
RUN_IN_REPO := cd "$(REPO_ROOT)" && mkdir -p $(ODDSFOX_RUNTIME_DIRS) &&
DUCKDB_NAME ?= oddsfox.duckdb
# Keep disposable dbt DuckDB files under ODDSFOX_RUNTIME_ROOT so parallel
# ci-fast / release-gate lanes do not share the same warehouse paths.
DBT_LINT_DUCKDB_PATH := $(ODDSFOX_RUNTIME_ROOT)/dbt_lint.duckdb
DBT_LINT_ENV := DUCKDB_PATH="$(DBT_LINT_DUCKDB_PATH)"
DBT_BUILD_DUCKDB_PATH := $(ODDSFOX_RUNTIME_ROOT)/dbt_build.duckdb
DBT_BUILD_ENV := DUCKDB_NAME="$(DBT_BUILD_DUCKDB_PATH)" DUCKDB_PATH="$(DBT_BUILD_DUCKDB_PATH)"
DBT_UNIT_DUCKDB_PATH := $(ODDSFOX_RUNTIME_ROOT)/dbt_unit.duckdb
DBT_UNIT_ENV := DUCKDB_NAME="$(DBT_UNIT_DUCKDB_PATH)" DUCKDB_PATH="$(DBT_UNIT_DUCKDB_PATH)"
DBT_FRESHNESS_DUCKDB_PATH := $(ODDSFOX_RUNTIME_ROOT)/dbt_source_freshness.duckdb
DBT_FRESHNESS_ENV := DUCKDB_NAME="$(DBT_FRESHNESS_DUCKDB_PATH)" DUCKDB_PATH="$(DBT_FRESHNESS_DUCKDB_PATH)"
DBT_DEPS_LOCK := $(REPO_ROOT)/.cache/runtime/dbt-deps.lock
MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH := $(REPO_ROOT)/.cache/match_minute_live_smoke.duckdb
# Isolate Parquet snapshots from the operator runtime root (same hazard as
# unified minute smoke: retain GC + absolute/CURRENT views).
MATCH_MINUTE_LIVE_SMOKE_RUNTIME_ROOT := $(REPO_ROOT)/.cache/runtime/smoke/match-minute-live
MATCH_MINUTE_LIVE_SMOKE_ENV := DUCKDB_NAME="$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)" DUCKDB_PATH="$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)" ODDSFOX_RUNTIME_ROOT="$(MATCH_MINUTE_LIVE_SMOKE_RUNTIME_ROOT)"
MINUTE_ODDS_LIVE_SMOKE_DUCKDB_PATH := $(REPO_ROOT)/.cache/minute_odds_live_smoke.duckdb
MINUTE_ODDS_LIVE_SMOKE_RESET ?= true
# Default true so cold smoke refreshes catalog even when operator .env has it false.
MINUTE_ODDS_LIVE_SMOKE_REFRESH_CATALOG ?= true
MINUTE_ODDS_LIVE_SMOKE_REPORT_PATH ?= $(ODDSFOX_RUNTIME_ROOT)/smoke/minute-odds/minute_odds_live_smoke.json
# Isolate Parquet snapshots from the operator runtime root so sampled smoke
# publishes cannot GC or shrink production CURRENT pointers.
MINUTE_ODDS_LIVE_SMOKE_RUNTIME_ROOT := $(REPO_ROOT)/.cache/runtime/smoke/minute-odds-live
# Force both minute legs on; catalog refresh stays overridable for warm reruns.
MINUTE_ODDS_LIVE_SMOKE_ENV := DUCKDB_NAME="$(MINUTE_ODDS_LIVE_SMOKE_DUCKDB_PATH)" DUCKDB_PATH="$(MINUTE_ODDS_LIVE_SMOKE_DUCKDB_PATH)" ODDSFOX_RUNTIME_ROOT="$(MINUTE_ODDS_LIVE_SMOKE_RUNTIME_ROOT)" POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG="$(MINUTE_ODDS_LIVE_SMOKE_REFRESH_CATALOG)" POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH=true POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES=true
MATCH_ORDER_BOOK_LIVE_SMOKE_DUCKDB_PATH := $(REPO_ROOT)/.cache/match_order_book_live_smoke.duckdb
MATCH_ORDER_BOOK_LIVE_SMOKE_ENV := DUCKDB_NAME="$(MATCH_ORDER_BOOK_LIVE_SMOKE_DUCKDB_PATH)" DUCKDB_PATH="$(MATCH_ORDER_BOOK_LIVE_SMOKE_DUCKDB_PATH)"
MATCH_ORDER_BOOK_LIVE_SMOKE_RESET ?= false
POLYGON_RUNTIME_ROOT := $(REPO_ROOT)/.cache/polygon_settlement
POLYGON_RUNTIME_TMP := $(POLYGON_RUNTIME_ROOT)/tmp
POLYGON_RUNTIME_XDG := $(POLYGON_RUNTIME_ROOT)/xdg
POLYGON_RUNTIME_DAGSTER_HOME := $(POLYGON_RUNTIME_ROOT)/dagster
POLYGON_RUNTIME_DBT_TARGET := $(POLYGON_RUNTIME_ROOT)/dbt-target
POLYGON_RUNTIME_DBT_LOGS := $(POLYGON_RUNTIME_ROOT)/dbt-logs
POLYGON_RUNTIME_PYCACHE := $(POLYGON_RUNTIME_ROOT)/pycache
POLYGON_RUNTIME_DUCKDB_EXTENSIONS := $(POLYGON_RUNTIME_ROOT)/duckdb-extensions
# Keep uv caches on the shared runtime root even when gate lanes override
# ODDSFOX_RUNTIME_ROOT for dbt/tmp isolation.
POLYGON_RUNTIME_UV := $(REPO_ROOT)/.cache/runtime/uv
POLYGON_RUNTIME_UV_PYTHON := $(REPO_ROOT)/.cache/runtime/uv-python
POLYGON_UNIT_DUCKDB_PATH := $(POLYGON_RUNTIME_ROOT)/dbt_unit.duckdb
POLYGON_UNIT_ENV := DUCKDB_NAME="$(POLYGON_UNIT_DUCKDB_PATH)" DUCKDB_PATH="$(POLYGON_UNIT_DUCKDB_PATH)"
POLYGON_RUNTIME_ENV := TMPDIR="$(POLYGON_RUNTIME_TMP)" XDG_CACHE_HOME="$(POLYGON_RUNTIME_XDG)" UV_CACHE_DIR="$(POLYGON_RUNTIME_UV)" UV_PYTHON_INSTALL_DIR="$(POLYGON_RUNTIME_UV_PYTHON)" DAGSTER_HOME="$(POLYGON_RUNTIME_DAGSTER_HOME)" DBT_TARGET_PATH="$(POLYGON_RUNTIME_DBT_TARGET)" DBT_LOG_PATH="$(POLYGON_RUNTIME_DBT_LOGS)" PYTHONPYCACHEPREFIX="$(POLYGON_RUNTIME_PYCACHE)" DUCKDB_EXTENSION_DIRECTORY="$(POLYGON_RUNTIME_DUCKDB_EXTENSIONS)" DBT_SEND_ANONYMOUS_USAGE_STATS=false
POLYGON_SETTLEMENT_LIVE_SMOKE_DUCKDB_PATH := $(POLYGON_RUNTIME_ROOT)/benchmarks/v4/live_smoke.duckdb
POLYGON_SETTLEMENT_LIVE_SMOKE_ENV := DUCKDB_NAME="$(POLYGON_SETTLEMENT_LIVE_SMOKE_DUCKDB_PATH)" DUCKDB_PATH="$(POLYGON_SETTLEMENT_LIVE_SMOKE_DUCKDB_PATH)"
POLYGON_SETTLEMENT_LIVE_SMOKE_RESET ?= false
POLYGON_SETTLEMENT_LIVE_SMOKE_REQUESTS_PER_SECOND ?= 5
POLYGON_SETTLEMENT_LIVE_SMOKE_WORKERS ?= 5
POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_BLOCK_CHUNK_SIZE ?= 8000
POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_RECEIPT_BATCH_SIZE ?= 20
POLYGON_SETTLEMENT_LIVE_SMOKE_TUNING_ENV := POLYGON_SETTLEMENT_LIVE_SMOKE_REQUESTS_PER_SECOND="$(POLYGON_SETTLEMENT_LIVE_SMOKE_REQUESTS_PER_SECOND)" POLYGON_SETTLEMENT_LIVE_SMOKE_WORKERS="$(POLYGON_SETTLEMENT_LIVE_SMOKE_WORKERS)" POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_BLOCK_CHUNK_SIZE="$(POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_BLOCK_CHUNK_SIZE)" POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_RECEIPT_BATCH_SIZE="$(POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_RECEIPT_BATCH_SIZE)"
POLYGON_BENCHMARK_V3_DUCKDB_PATH ?= $(POLYGON_RUNTIME_ROOT)/benchmarks/v3/live_smoke.duckdb
POLYGON_BENCHMARK_V4_DUCKDB_PATH ?= $(POLYGON_RUNTIME_ROOT)/benchmarks/v4/live_smoke.duckdb
POLYGON_BENCHMARK_REPORT_PATH ?= $(POLYGON_RUNTIME_ROOT)/benchmarks/v4/benchmark.json
FUTURES_MINUTE_PUBLISH_BENCHMARK_ROOT := $(ODDSFOX_RUNTIME_ROOT)/benchmarks/futures-minute-publish
FUTURES_MINUTE_PUBLISH_BENCHMARK_TIER ?= smoke
FUTURES_MINUTE_PUBLISH_BENCHMARK_REPORT_PATH ?= $(FUTURES_MINUTE_PUBLISH_BENCHMARK_ROOT)/$(FUTURES_MINUTE_PUBLISH_BENCHMARK_TIER).json
FUTURES_MINUTE_PUBLISH_BENCHMARK_REQUIRE_SPEEDUP ?= 0
FUTURES_MINUTE_PUBLISH_BENCHMARK_MATRIX ?= false
MINUTE_ODDS_DBT_BENCHMARK_ROOT := $(ODDSFOX_RUNTIME_ROOT)/benchmarks/minute-odds-dbt
MINUTE_ODDS_DBT_BENCHMARK_TIER ?= performance
MINUTE_ODDS_DBT_BENCHMARK_REPORT_PATH ?= $(MINUTE_ODDS_DBT_BENCHMARK_ROOT)/$(MINUTE_ODDS_DBT_BENCHMARK_TIER).json
MINUTE_ODDS_DBT_BENCHMARK_THREADS ?= 2
POLYGON_SEED_MANIFEST_VERSION ?=
POLYGON_SEED_REVIEWED_AT ?=
POLYGON_SEED_OUTPUT_DIR ?= artifacts/polygon_settlement_seed_candidates/$(POLYGON_SEED_MANIFEST_VERSION)
POLYGON_DATASET_VERSION ?=
POLYGON_AUDIT_OUTPUT_ROOT ?= artifacts/polygon_settlement/audit
POLYGON_EXPORT_OUTPUT_ROOT ?= artifacts/polygon_settlement/exports
GRAPH_NODES_PATH ?=
GRAPH_EDGES_PATH ?=
GRAPH_REVISION ?=
STAGE_MINUTE_DATASET_VERSION ?= 1.0.0
STAGE_MINUTE_OUTPUT_ROOT ?= artifacts/strategy-inputs/polymarket_wc2026_stage_minute
STAGE_EXECUTION_DATASET_VERSION ?= 1.0.0
STAGE_EXECUTION_OUTPUT_ROOT ?= artifacts/strategy-inputs/polymarket_wc2026_stage_execution
STAGE_EXECUTION_MINUTE_RELEASE ?=
STAGE_EXECUTION_OHLC_REPORT ?=
STAGE_EXECUTION_REQUEST_BUDGET ?= 20000
STAGE_EXECUTION_SOURCE ?= archive-v2
STAGE_EXECUTION_CREDIT_LEDGER ?= $(DUCKDB_NAME)
MATCH_ANALYSIS_RUNTIME_ROOT ?= $(REPO_ROOT)/.cache/match_analysis
MATCH_ANALYSIS_RUNTIME_TMP := $(MATCH_ANALYSIS_RUNTIME_ROOT)/tmp
MATCH_ANALYSIS_RUNTIME_XDG := $(MATCH_ANALYSIS_RUNTIME_ROOT)/xdg
MATCH_ANALYSIS_RUNTIME_DBT_TARGET := $(MATCH_ANALYSIS_RUNTIME_ROOT)/dbt-target
MATCH_ANALYSIS_RUNTIME_DBT_LOGS := $(MATCH_ANALYSIS_RUNTIME_ROOT)/dbt-logs
MATCH_ANALYSIS_RUNTIME_PYCACHE := $(MATCH_ANALYSIS_RUNTIME_ROOT)/pycache
MATCH_ANALYSIS_RUNTIME_DUCKDB_EXTENSIONS := $(MATCH_ANALYSIS_RUNTIME_ROOT)/duckdb-extensions
MATCH_ANALYSIS_RUNTIME_ENV := TMPDIR="$(MATCH_ANALYSIS_RUNTIME_TMP)" XDG_CACHE_HOME="$(MATCH_ANALYSIS_RUNTIME_XDG)" UV_CACHE_DIR="$(POLYGON_RUNTIME_UV)" UV_PYTHON_INSTALL_DIR="$(POLYGON_RUNTIME_UV_PYTHON)" DBT_TARGET_PATH="$(MATCH_ANALYSIS_RUNTIME_DBT_TARGET)" DBT_LOG_PATH="$(MATCH_ANALYSIS_RUNTIME_DBT_LOGS)" PYTHONPYCACHEPREFIX="$(MATCH_ANALYSIS_RUNTIME_PYCACHE)" DUCKDB_EXTENSION_DIRECTORY="$(MATCH_ANALYSIS_RUNTIME_DUCKDB_EXTENSIONS)" DBT_SEND_ANONYMOUS_USAGE_STATS=false
MINUTE_ODDS_UNIT_DUCKDB_PATH := $(MATCH_ANALYSIS_RUNTIME_ROOT)/dbt_unit_minute_odds.duckdb
MINUTE_ODDS_UNIT_ENV := DUCKDB_NAME="$(MINUTE_ODDS_UNIT_DUCKDB_PATH)" DUCKDB_PATH="$(MINUTE_ODDS_UNIT_DUCKDB_PATH)"
SOCCER_MINUTE_LIVE_SMOKE_DUCKDB_PATH := $(REPO_ROOT)/.cache/soccer_minute_live_smoke.duckdb
SOCCER_MINUTE_LIVE_SMOKE_RUNTIME_ROOT := $(REPO_ROOT)/.cache/runtime/smoke/soccer-minute-live
SOCCER_MINUTE_LIVE_SMOKE_ENV := DUCKDB_NAME="$(SOCCER_MINUTE_LIVE_SMOKE_DUCKDB_PATH)" DUCKDB_PATH="$(SOCCER_MINUTE_LIVE_SMOKE_DUCKDB_PATH)" ODDSFOX_RUNTIME_ROOT="$(SOCCER_MINUTE_LIVE_SMOKE_RUNTIME_ROOT)"
SOCCER_MINUTE_PERFORMANCE_ROOT := $(ODDSFOX_RUNTIME_ROOT)/benchmarks/polymarket-soccer
SOCCER_MINUTE_PERFORMANCE_REPORT_PATH ?= $(SOCCER_MINUTE_PERFORMANCE_ROOT)/performance.json
MATCH_MINUTE_REBUILD_DUCKDB_PATH ?=
POLYGON_SETTLEMENT_REBUILD_DUCKDB_PATH ?=
PYTEST_FAST_MARKERS := not integration and not performance and not slow and not repo_check and not contract
PYTEST_COVERAGE_MARKERS := not performance and not slow and not repo_check and not contract
PYTEST_UNIT_IGNORES := --ignore=tests/integration --ignore=tests/contract --ignore=tests/repository --ignore=tests/docs --ignore=tests/package
PYTEST_DURATION_ARGS ?= --durations=25
GATE_JOBS ?= 4
DBT_TEST_WORKERS ?= 2
RELEASE_PYTEST_WORKERS ?= 2
MUTMUT_MAX_CHILDREN ?= 2
# --cov-report=: shards must stay quiet; release-gate combines then reports once.
COV_ARGS := --cov=oddsfox_pipeline --cov-branch --cov-report=
COV_APPEND_ARGS := $(COV_ARGS) --cov-append
CI_FAST_STATIC_RUNTIME := $(REPO_ROOT)/.cache/runtime/ci-fast-static
CI_FAST_TESTS_RUNTIME := $(REPO_ROOT)/.cache/runtime/ci-fast-tests
CI_FAST_DBT_RUNTIME := $(REPO_ROOT)/.cache/runtime/ci-fast-dbt
RELEASE_COVERAGE_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-coverage
RELEASE_DBT_UNIT_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-dbt-unit
RELEASE_DBT_FRESHNESS_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-dbt-freshness
RELEASE_DBT_BUILD_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-dbt-build
RELEASE_DBT_MATCH_ORDER_BOOK_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-dbt-match-order-book
RELEASE_DBT_MATCH_MINUTE_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-dbt-match-minute
RELEASE_DBT_MINUTE_ODDS_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-dbt-minute-odds
RELEASE_DBT_MARKET_PORTRAIT_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-dbt-market-portrait
RELEASE_MUTATION_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-mutation
RELEASE_STATIC_RUNTIME := $(REPO_ROOT)/.cache/runtime/release-static
COVERAGE_DATA_DIR := $(RELEASE_COVERAGE_RUNTIME)/coverage-data
TEST_DEV_PYTEST_ARGS ?=

runtime-dirs:
	@mkdir -p $(ODDSFOX_RUNTIME_DIRS)

include Makefile.gates
include Makefile.dbt
include Makefile.lint
include Makefile.test
include Makefile.ops

.PHONY: stage-execution-plan stage-execution-release
