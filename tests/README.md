# tests

This subtree validates OddsFox Pipeline. Version `0.1.x`
ships WC2026 and US midterms 2026 Polymarket ingestion, marts, and orchestration.

See [OddsFox Pipeline docs](../docs/index.md) for setup and runbook commands.

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

The ordinary `make test` suite uses xdist and excludes `tests/integration`,
`tests/dbt`, and `tests/contract`; those paths retain dedicated serial targets.
The full local release gate accumulates coverage with `make test-cov`,
`make dagster-jobs-smoke-cov`, `make dagster-refresh-cov`,
`make integration-dbt-cov`, and `make coverage-report`, alongside the dbt,
freshness, golden, and data-quality targets. `make integration-dagster-cov`
wraps both split Dagster coverage targets, while `make coverage` is the
one-shot equivalent. Local gates invoke these commands sequentially. GitHub's
automatic `tests` worker runs the parallel fast suite and serial
`make contract-http` while independent static/docs and dbt-lint workers run in
parallel; the `contract` marker remains excluded from `make test` and
`make test-cov`.

`make dagster-jobs-smoke` runs every registered public Dagster job headlessly
with temp DuckDB state and mocked external APIs. The local coverage gate splits
that registered-job smoke from the deeper seeded Dagster refresh path without
enabling xdist on DuckDB/Dagster fixtures. Together with the other coverage
commands, they enforce 100% branch coverage for `src/oddsfox_pipeline` except
the warehouse profiling operator helpers under `storage/duckdb/profile/`, which
are covered by smoke tests instead.

`make gx-data-quality` runs against an existing disposable
`.cache/dbt_build.duckdb` database and writes Great Expectations report artifacts
under `.cache/`. `make data-quality` is the safe local wrapper that rebuilds
that disposable dbt state first. Generated reports are local artifacts and
should not be committed.

When `.env` sets `DUCKDB_PATH`, use `isolate_duckdb_test_env()` from
`tests/unit/storage/duckdb_storage_test_support.py` so tests do not write to the
production warehouse. See
[Development](../docs/development/index.md#local-env-and-tests).
