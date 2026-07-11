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
make dagster-jobs-smoke-cov
make dagster-refresh-cov
make dbt-unit
make golden-dbt
make dbt-source-freshness-ci
make gx-data-quality
make data-quality
make integration-dbt
make integration-dagster
make contract-http
make test
make coverage
make check-secrets
```

CI mirrors the quality gate with coverage accumulation and parallel jobs:
`make test-cov`, `make dagster-jobs-smoke-cov`, `make dagster-refresh-cov`,
`make integration-dbt-cov`, `make dbt-unit`, `make golden-dbt`,
`make dbt-source-freshness-ci`, `make gx-data-quality`, and
`make coverage-report`. Local `make integration-dagster-cov` remains a wrapper
around both split Dagster coverage targets, and local `make coverage` is still a
one-shot equivalent. `make contract-http` is manual/nightly; the `contract`
marker is excluded from `make test`, `make test-cov`, and default CI.

`make dagster-jobs-smoke` runs every registered public Dagster job headlessly
with temp DuckDB state and mocked external APIs. CI splits that registered-job
smoke from the deeper seeded Dagster refresh-path smoke so the jobs can run in
parallel without enabling xdist on DuckDB/Dagster fixtures. Together with the
other coverage jobs, they enforce 100% branch coverage for `src/oddsfox_pipeline`
except the warehouse profiling operator helpers under `storage/duckdb/profile/`,
which are covered by smoke tests instead.

`make gx-data-quality` runs against an existing disposable
`.cache/dbt_build.duckdb` database and writes Great Expectations report artifacts
under `.cache/`. `make data-quality` is the safe local wrapper that rebuilds
that disposable dbt state first. Generated reports are local artifacts and
should not be committed.

When `.env` sets `DUCKDB_PATH`, use `isolate_duckdb_test_env()` from
`tests/unit/storage/duckdb_storage_test_support.py` so tests do not write to the
production warehouse. See [Development](../docs/development.md#local-env-and-tests).
