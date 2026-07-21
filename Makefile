.PHONY: ci-fast release-gate release-gate-core container-smoke container-smoke-run dagster-dev dagster-jobs-smoke dagster-jobs-smoke-cov dagster-refresh-cov duckdb-ui dbt-build dbt-build-ci dbt-parse dbt-test dbt-unit dbt-source-freshness-ci golden-dbt gx-data-quality data-quality contract-http live-smoke match-minute-live-smoke costguard costguard-scan docs-serve docs-build docs-test docs-check clean-local-artifacts format lint test test-cov coverage coverage-erase coverage-report unit-core unit-ingest unit-orchestration integration-dbt integration-dbt-cov integration-dagster integration-dagster-cov check-secrets compact-warehouse prune-odds-history

REPO_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
override PYTHON := $(shell if test -x "$(REPO_ROOT)/.venv/bin/python"; then printf '%s' "$(REPO_ROOT)/.venv/bin/python"; else printf 'python3'; fi)
COSTGUARD ?= costguard
RUN_IN_REPO := cd "$(REPO_ROOT)" &&
DUCKDB_NAME ?= oddsfox.duckdb
DBT_LINT_DUCKDB_PATH := $(REPO_ROOT)/.cache/dbt_lint.duckdb
DBT_LINT_ENV := DUCKDB_PATH="$(DBT_LINT_DUCKDB_PATH)"
DBT_BUILD_DUCKDB_PATH := $(REPO_ROOT)/.cache/dbt_build.duckdb
DBT_BUILD_ENV := DUCKDB_NAME="$(DBT_BUILD_DUCKDB_PATH)" DUCKDB_PATH="$(DBT_BUILD_DUCKDB_PATH)"
DBT_UNIT_DUCKDB_PATH := $(REPO_ROOT)/.cache/dbt_unit.duckdb
DBT_UNIT_ENV := DUCKDB_NAME="$(DBT_UNIT_DUCKDB_PATH)" DUCKDB_PATH="$(DBT_UNIT_DUCKDB_PATH)"
DBT_FRESHNESS_DUCKDB_PATH := $(REPO_ROOT)/.cache/dbt_source_freshness.duckdb
DBT_FRESHNESS_ENV := DUCKDB_NAME="$(DBT_FRESHNESS_DUCKDB_PATH)" DUCKDB_PATH="$(DBT_FRESHNESS_DUCKDB_PATH)"
MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH := $(REPO_ROOT)/.cache/match_minute_live_smoke.duckdb
MATCH_MINUTE_LIVE_SMOKE_ENV := DUCKDB_NAME="$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)" DUCKDB_PATH="$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)"
PYTEST_FAST_MARKERS := not integration and not performance and not slow and not repo_check and not contract
PYTEST_COVERAGE_MARKERS := not performance and not slow and not repo_check and not contract
COV_APPEND_ARGS := --cov=oddsfox_pipeline --cov-branch --cov-append
IMAGE ?= oddsfox-pipeline:ci
VCS_REF ?= $(shell git -C "$(REPO_ROOT)" rev-parse HEAD)

ci-fast:
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) contract-http
	$(MAKE) dbt-parse
	$(MAKE) docs-build

release-gate:
	$(MAKE) release-gate-core
	$(MAKE) container-smoke

release-gate-core:
	$(MAKE) ci-fast
	$(MAKE) test-cov
	$(MAKE) dagster-jobs-smoke-cov
	$(MAKE) dagster-refresh-cov
	$(MAKE) integration-dbt-cov
	$(MAKE) dbt-unit
	$(MAKE) golden-dbt
	$(MAKE) dbt-source-freshness-ci
	$(MAKE) coverage-report
	$(MAKE) docs-test
	$(MAKE) dbt-build-ci
	$(MAKE) gx-data-quality
	$(MAKE) costguard-scan

container-smoke:
	docker buildx build --load --tag "$(IMAGE)" --build-arg "VCS_REF=$(VCS_REF)" .
	$(MAKE) container-smoke-run

container-smoke-run:
	docker run --rm \
		--read-only \
		--cap-drop ALL \
		--security-opt no-new-privileges:true \
		--tmpfs /tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001 \
		--tmpfs /runtime/warehouse:rw,noexec,nosuid,size=128m,uid=10001,gid=10001 \
		"$(IMAGE)" \
		python -c "from oddsfox_pipeline.config import settings; assert settings.DBT_PROJECT_DIR.is_dir(); print(settings.DBT_PROJECT_DIR)"

duckdb-ui:
	duckdb "$(REPO_ROOT)/$(DUCKDB_NAME)" -ui

dagster-dev:
	mkdir -p "$(REPO_ROOT)/.dagster_home"
	cp "$(REPO_ROOT)/dagster_instance.yaml" "$(REPO_ROOT)/.dagster_home/dagster.yaml"
	cd "$(REPO_ROOT)" && \
		export PATH="$(REPO_ROOT)/.venv/bin:$$PATH" && \
		export DAGSTER_HOME="$(REPO_ROOT)/.dagster_home" && \
		if test -x "$(REPO_ROOT)/.venv/bin/dg"; then \
			"$(REPO_ROOT)/.venv/bin/dg" dev -h 127.0.0.1 -w "$(REPO_ROOT)/workspace.yaml"; \
		else \
			"$(PYTHON)" -m dagster dev -h 127.0.0.1 -w "$(REPO_ROOT)/workspace.yaml"; \
		fi

dbt-build dbt-test:
	$(RUN_IN_REPO) "$(PYTHON)" -m dbt.cli.main build --project-dir dbt --profiles-dir dbt/profiles

dbt-build-ci:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) rm -f "$(DBT_BUILD_DUCKDB_PATH)" "$(DBT_BUILD_DUCKDB_PATH).wal" "$(DBT_BUILD_DUCKDB_PATH)-wal" "$(DBT_BUILD_DUCKDB_PATH)-shm"
	$(RUN_IN_REPO) $(DBT_BUILD_ENV) "$(PYTHON)" -c "import oddsfox_pipeline.storage.duckdb.connection as connection; from oddsfox_pipeline.storage.duckdb.schemas.kalshi import create_all_kalshi_test_raw_tables, seed_test_kalshi_pipeline_run_event; from oddsfox_pipeline.storage.duckdb.schemas.polymarket import create_all_scope_test_markets_tables, seed_test_pipeline_run_event; connection.reset_duckdb_connection_state(); connection.init_duck_db(); conn = connection.get_persistent_connection(); create_all_scope_test_markets_tables(conn); seed_test_pipeline_run_event(conn); create_all_kalshi_test_raw_tables(conn); seed_test_kalshi_pipeline_run_event(conn); conn.close()"
	$(RUN_IN_REPO) $(DBT_BUILD_ENV) $(MAKE) dbt-build

dbt-parse:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt/profiles

dbt-unit:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) rm -f "$(DBT_UNIT_DUCKDB_PATH)" "$(DBT_UNIT_DUCKDB_PATH).wal" "$(DBT_UNIT_DUCKDB_PATH)-wal" "$(DBT_UNIT_DUCKDB_PATH)-shm"
	$(RUN_IN_REPO) $(DBT_UNIT_ENV) "$(PYTHON)" -c "import oddsfox_pipeline.storage.duckdb.connection as connection; from oddsfox_pipeline.storage.duckdb.schemas.kalshi import create_all_kalshi_test_raw_tables, seed_test_kalshi_pipeline_run_event; from oddsfox_pipeline.storage.duckdb.schemas.polymarket import create_all_scope_test_markets_tables, seed_test_pipeline_run_event; connection.reset_duckdb_connection_state(); connection.init_duck_db(); conn = connection.get_persistent_connection(); create_all_scope_test_markets_tables(conn); seed_test_pipeline_run_event(conn); create_all_kalshi_test_raw_tables(conn); seed_test_kalshi_pipeline_run_event(conn); conn.close()"
	$(RUN_IN_REPO) $(DBT_UNIT_ENV) "$(PYTHON)" -m dbt.cli.main seed --project-dir dbt --profiles-dir dbt/profiles
	$(RUN_IN_REPO) $(DBT_UNIT_ENV) "$(PYTHON)" -m dbt.cli.main run --empty --project-dir dbt --profiles-dir dbt/profiles
	$(RUN_IN_REPO) $(DBT_UNIT_ENV) "$(PYTHON)" -m dbt.cli.main test --select "test_type:unit" --project-dir dbt --profiles-dir dbt/profiles

dbt-source-freshness-ci:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) rm -f "$(DBT_FRESHNESS_DUCKDB_PATH)" "$(DBT_FRESHNESS_DUCKDB_PATH).wal" "$(DBT_FRESHNESS_DUCKDB_PATH)-wal" "$(DBT_FRESHNESS_DUCKDB_PATH)-shm"
	$(RUN_IN_REPO) $(DBT_FRESHNESS_ENV) "$(PYTHON)" scripts/seed_dbt_source_freshness.py
	$(RUN_IN_REPO) $(DBT_FRESHNESS_ENV) "$(PYTHON)" -m dbt.cli.main source freshness --project-dir dbt --profiles-dir dbt/profiles

golden-dbt:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb/test_golden_marts.py -q -n 0 -m "not performance and not slow"

gx-data-quality:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/run_gx_data_quality.py --duckdb-path "$(DBT_BUILD_DUCKDB_PATH)"

data-quality: dbt-build-ci gx-data-quality

contract-http:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/contract -q -n 0 -m "contract"

live-smoke:
	$(RUN_IN_REPO) "$(PYTHON)" -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j wc2026_knockout_match_odds_full_pipeline -c config/live-smoke.yaml

match-minute-live-smoke:
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) rm -f "$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)" "$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH).wal" "$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)-wal" "$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)-shm"
	cd "$(REPO_ROOT)/.cache" && $(MATCH_MINUTE_LIVE_SMOKE_ENV) "$(PYTHON)" -m dagster job execute -d "$(REPO_ROOT)" -m oddsfox_pipeline.orchestration.definitions -j polymarket_wc2026_match_minute_odds_backfill
	$(RUN_IN_REPO) $(MATCH_MINUTE_LIVE_SMOKE_ENV) "$(PYTHON)" -c "import duckdb; conn = duckdb.connect('$(MATCH_MINUTE_LIVE_SMOKE_DUCKDB_PATH)', read_only=True); row = conn.execute('select mapped_games, mapped_markets, mapped_group_markets, mapped_knockout_markets, mapped_tokens, international_results_games, international_results_mapped_games, international_results_mapped_source_games, international_results_revisions, international_results_payload_hashes, international_results_provenance_issues, latest_fetch_run_status, latest_fetch_audited_tokens, latest_fetch_success_tokens, latest_fetch_empty_tokens, latest_fetch_error_tokens, latest_fetch_cancelled_tokens, latest_fetch_published_tokens, latest_fetch_hash_issues, elapsed_axis_issue_markets, error_issue_count, blocking_issue_keys from polymarket_wc2026_observability.polymarket_wc2026_match_minute_odds_data_quality').fetchone(); expected = (104, 248, 216, 32, 496, 104, 104, 104, 1, 1, 0, 'published', 496, 496, 0, 0, 0, 496, 0, 0, 0, None); assert row == expected, row; print(row)"

costguard-scan:
	$(RUN_IN_REPO) cd dbt && "$(COSTGUARD)" scan

costguard: dbt-build-ci costguard-scan

docs-serve:
	$(RUN_IN_REPO) NO_MKDOCS_2_WARNING=true "$(PYTHON)" -m mkdocs serve -a 127.0.0.1:8000

docs-build:
	$(RUN_IN_REPO) NO_MKDOCS_2_WARNING=true "$(PYTHON)" -m mkdocs build --strict

docs-test:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/test_docs_structure.py tests/test_docs_render.py -q -n 0

docs-check: docs-build docs-test

format:
	$(RUN_IN_REPO) ruff format src tests
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt/profiles
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m sqlfluff fix dbt/models dbt/tests

lint:
	$(RUN_IN_REPO) ruff format --check src tests
	$(RUN_IN_REPO) ruff check src tests
	$(RUN_IN_REPO) mkdir -p "$(REPO_ROOT)/.cache"
	$(RUN_IN_REPO) $(DBT_LINT_ENV) "$(PYTHON)" -m sqlfluff lint dbt/models dbt/tests -p 0
	$(MAKE) check-secrets

check-secrets:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/test_secrets_not_committed.py -q -n 0

test:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests -q -m "$(PYTEST_FAST_MARKERS)"

coverage-erase:
	$(RUN_IN_REPO) "$(PYTHON)" -m coverage erase

test-cov: coverage-erase
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests --ignore=tests/integration -q -n auto -m "$(PYTEST_FAST_MARKERS)" $(COV_APPEND_ARGS)
	# ponytail: tests/conftest.py auto-marks tests/integration/* as integration;
	# run ingestion integration serially here so CI coverage matches make coverage.
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/ingestion -q -n 0 -m "not performance and not slow" $(COV_APPEND_ARGS)

coverage:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests -q -m "$(PYTEST_COVERAGE_MARKERS)" --cov=oddsfox_pipeline --cov-branch --cov-report=term-missing --cov-fail-under=100

coverage-report:
	$(RUN_IN_REPO) "$(PYTHON)" -m coverage report --show-missing --fail-under=100

unit-core:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/unit/config tests/unit/resources tests/unit/storage -q -n 0 -m "$(PYTEST_FAST_MARKERS)"

unit-ingest:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/unit/ingestion -q -n 0 -m "$(PYTEST_FAST_MARKERS)"

unit-orchestration:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/unit/orchestration -q -n 0 -m "not performance and not slow"

dagster-jobs-smoke:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster/test_registered_jobs_smoke.py -q -n 0 -m "not performance and not slow"

dagster-jobs-smoke-cov:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster/test_registered_jobs_smoke.py -q -n 0 -m "not performance and not slow" $(COV_APPEND_ARGS)

dagster-refresh-cov:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster/test_refresh_job_smoke.py -q -n 0 -m "not performance and not slow" $(COV_APPEND_ARGS)

integration-dbt:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb tests/dbt -q -n 0 -m "not performance and not slow"

integration-dbt-cov:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb tests/dbt -q -n 0 -m "not performance and not slow" $(COV_APPEND_ARGS)

integration-dagster:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster -q -n 0 -m "not performance and not slow"

# ponytail: keep each Dagster group serial (-n 0) until xdist safety is proven
# for Dagster instance and DuckDB-locked fixtures.
integration-dagster-cov: dagster-jobs-smoke-cov dagster-refresh-cov

clean-local-artifacts:
	$(RUN_IN_REPO) find . -type d -name __pycache__ -prune -exec rm -rf {} +
	$(RUN_IN_REPO) rm -rf .pytest_cache .ruff_cache .dagster_home .cache site dbt/logs dbt/target src/oddsfox_pipeline.egg-info
	$(RUN_IN_REPO) find . -maxdepth 2 \( -name '*.duckdb' -o -name '*.duckdb.tmp' -o -name '*.duckdb-wal' -o -name '*.duckdb-shm' -o -name '*.duckdb.wal' \) -exec rm -rf {} +

compact-warehouse:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/compact_warehouse.py

prune-odds-history:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/prune_odds_history.py
