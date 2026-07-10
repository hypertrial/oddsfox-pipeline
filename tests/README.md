# tests

This subtree validates the OddsFox prediction-market pipeline. Version `0.1.x`
ships WC2026 and US midterms 2026 Polymarket ingestion, marts, and orchestration.

See [OddsFox docs](../docs/index.md) for setup and runbook commands.

- `unit/`: mocked config, ingestion, storage, and orchestration tests.
- `integration/`: DuckDB/dbt/Dagster smoke tests using temp databases.
- `dbt/`: dbt project structure checks.
- `contract/`: replay-only HTTP contract tests using checked-in VCR cassettes.
- top-level tests: repository policy checks such as secret scanning.

Useful commands:

```bash
make unit-core
make unit-ingest
make unit-orchestration
make dagster-jobs-smoke
make dbt-unit
make golden-dbt
make dbt-source-freshness-ci
make data-quality
make integration-dbt
make integration-dagster
make contract-http
make test
make coverage
```

CI mirrors the quality gate with coverage accumulation:
`make test-cov`, `make integration-dagster-cov`, `make integration-dbt-cov`,
`make dbt-unit`, `make golden-dbt`, `make dbt-source-freshness-ci`,
`make data-quality`, and `make coverage-report`. Local `make coverage` is still
a one-shot equivalent. `make contract-http` is manual/nightly; the `contract`
marker is excluded from `make test`, `make test-cov`, and default CI.

`make dagster-jobs-smoke` runs every registered public Dagster job headlessly
with temp DuckDB state and mocked external APIs. `make integration-dagster` and
the CI `*-cov` targets include that smoke plus deeper seeded Dagster runs and
enforce 100% branch coverage for `src/oddsfox_pipeline` except the warehouse
profiling operator helpers under `storage/duckdb/profile/`, which are covered by
smoke tests instead.

`make data-quality` runs against the disposable `.cache/dbt_build.duckdb`
database and writes Great Expectations report artifacts under `.cache/`.
Generated reports are local artifacts and should not be committed.

When `.env` sets `DUCKDB_PATH`, use `isolate_duckdb_test_env()` from
`tests/unit/storage/duckdb_storage_test_support.py` so tests do not write to the
production warehouse. See [Development](../docs/development.md#local-env-and-tests).
