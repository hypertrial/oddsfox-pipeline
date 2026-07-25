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
make data-quality
make mutation
make mutation-ci
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
freshness, golden, data-quality, and focused mutation targets. `make
integration-dagster-cov` wraps both split Dagster coverage targets, while `make
coverage` is the one-shot equivalent. Local gates invoke these commands
sequentially. GitHub's automatic `tests` worker runs the parallel fast suite and
serial `make contract-http` while independent static/docs and dbt-lint workers
run in parallel. A required Python 3.13 worker repeats package smoke and the
ordinary suite while Python 3.10 remains the supported floor and full-release
runtime. The `contract` marker remains excluded from `make test` and
`make test-cov`.

`make dagster-jobs-smoke` runs every registered public Dagster job headlessly
with temp DuckDB state and mocked external APIs. The local coverage gate splits
that registered-job smoke from the deeper seeded Dagster refresh path without
enabling xdist on DuckDB/Dagster fixtures. Together with the other coverage
commands, they enforce 100% branch coverage for `src/oddsfox_pipeline` except
the warehouse profiling operator helpers under `storage/duckdb/profile/`, which
are covered by smoke tests instead.

`make data-quality` rebuilds disposable dbt state and runs the dbt-native model
and data tests. `make mutation` resumes cached focused Mutmut work; `make
mutation-ci` deletes `mutants/` first and is the deterministic release gate.
Its five-module scope covers outbound URL safety, raw snapshot contracts,
market-scope predicates, market persistence, and odds planning. Mutation output
is local or a short-lived Manual Full Validation artifact and must not be
committed.

The dbt integration suite requires incremental/full-refresh equivalence for
every incremental odds model, including late, null-refresh, new-key, uniqueness,
and retention cases. Seeded Dagster integration tests replay the WC2026
Polymarket, US midterms Polymarket, and Kalshi refresh paths twice and compare
stable business state. The Polymarket path also injects a second-flush
transaction failure and requires replay to match a clean uninterrupted run.

When `.env` sets `DUCKDB_PATH`, use `isolate_duckdb_test_env()` from
`tests/unit/storage/duckdb_storage_test_support.py` so tests do not write to the
production warehouse. See
[Development](../docs/development/index.md#local-env-and-tests).
