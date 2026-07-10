.PHONY: dagster-dev dagster-jobs-smoke duckdb-ui dbt-build dbt-build-ci dbt-parse dbt-test dbt-unit dbt-source-freshness-ci golden-dbt data-quality contract-http costguard docs-serve docs-build docs-check clean-local-artifacts format lint test test-cov coverage coverage-erase coverage-report unit-core unit-ingest unit-orchestration integration-dbt integration-dbt-cov integration-dagster integration-dagster-cov check-secrets compact-warehouse prune-odds-history

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
PYTEST_FAST_MARKERS := not integration and not performance and not slow and not repo_check and not contract
PYTEST_COVERAGE_MARKERS := not performance and not slow and not repo_check and not contract
COV_APPEND_ARGS := --cov=oddsfox_pipeline --cov-branch --cov-append

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

data-quality: dbt-build-ci
	$(RUN_IN_REPO) "$(PYTHON)" scripts/run_gx_data_quality.py --duckdb-path "$(DBT_BUILD_DUCKDB_PATH)"

contract-http:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/contract -q -n 0 -m "contract"

costguard: dbt-build-ci
	$(RUN_IN_REPO) cd dbt && "$(COSTGUARD)" scan

docs-serve:
	$(RUN_IN_REPO) NO_MKDOCS_2_WARNING=true "$(PYTHON)" -m mkdocs serve -a 127.0.0.1:8000

docs-build docs-check:
	$(RUN_IN_REPO) NO_MKDOCS_2_WARNING=true "$(PYTHON)" -m mkdocs build --strict
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/test_docs_structure.py::test_built_docs_use_material_homepage -q -n 0

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

integration-dbt:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb tests/dbt -q -n 0 -m "not performance and not slow"

integration-dbt-cov:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/duckdb tests/dbt -q -n 0 -m "not performance and not slow" $(COV_APPEND_ARGS)

integration-dagster:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster -q -n 0 -m "not performance and not slow"

# ponytail: integration-dagster/-dbt stay serial (-n 0) to match the existing
# guard on Dagster-instance/DuckDB-locked suites; revisit only after validating
# xdist safety for those fixtures.
integration-dagster-cov:
	$(RUN_IN_REPO) "$(PYTHON)" -m pytest tests/integration/dagster -q -n 0 -m "not performance and not slow" $(COV_APPEND_ARGS)

clean-local-artifacts:
	$(RUN_IN_REPO) find . -type d -name __pycache__ -prune -exec rm -rf {} +
	$(RUN_IN_REPO) rm -rf .pytest_cache .ruff_cache .dagster_home .cache site dbt/logs dbt/target src/oddsfox_pipeline.egg-info
	$(RUN_IN_REPO) find . -maxdepth 2 \( -name '*.duckdb' -o -name '*.duckdb.tmp' -o -name '*.duckdb-wal' -o -name '*.duckdb-shm' -o -name '*.duckdb.wal' \) -exec rm -rf {} +

compact-warehouse:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/compact_warehouse.py

prune-odds-history:
	$(RUN_IN_REPO) "$(PYTHON)" scripts/prune_odds_history.py
