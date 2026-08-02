# tests

This subtree validates OddsFox Pipeline. Version `0.1.x`
ships WC2026 and US midterms 2026 Polymarket ingestion, marts, and orchestration.

See [OddsFox Pipeline docs](../docs/index.md) for setup and runbook commands.

Ownership (paths) and execution properties (markers) are separate:

- `unit/`: mocked config, ingestion, storage, orchestration, publishing, and scripts.
- `integration/`: DuckDB/dbt/Dagster smoke tests using disposable databases.
- `repository/`: repository policy checks (Make, workflows, naming, distribution,
  terminology, secrets, static dbt project structure).
- `docs/`: documentation structure and rendered-page checks.
- `package/`: package distribution smoke.
- `contract/`: replay-only HTTP contract tests using checked-in VCR cassettes.

Markers retained for execution filtering: `integration`, `contract`, `slow`,
`performance`, `repo_check`, `facade`, and `polygon`.

`polygon` is auto-applied to any test file whose path contains `polygon`; do not
hand-add it to new tests. `make test-dev` may append `and not polygon` when the
branch diff (vs `origin/main`) touches no Polygon paths or shared infra.

`HYPOTHESIS_PROFILE` selects the Hypothesis example budget (`default`: 100,
`dev`: 15). `make test-dev` sets `HYPOTHESIS_PROFILE=dev`; other targets leave
the default profile.

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
make test-dev
make coverage
make check-repository
```

The ordinary `make test` suite uses xdist and excludes `tests/integration`,
`tests/contract`, `tests/repository`, `tests/docs`, and `tests/package`; those
paths retain dedicated targets. `make check-repository` runs the `repo_check`
suite (and depends on `dbt-prepare` where naming/static dbt checks need the
manifest). `make test` / `make test-cov` first run `dbt-prepare` so xdist
workers reuse one shared dbt manifest under `DBT_TARGET_PATH`.
`make integration-dbt` splits isolated incremental cases (`DBT_TEST_WORKERS`,
default 2) from the remaining serial DuckDB/dbt suite, which includes the golden
marts. Standalone `make golden-dbt` remains available but is not duplicated in
the release gate.

Ordinary work uses `ci-fast`. `release-gate` is for major-version publishes
only. Both use one Make jobserver (`GATE_JOBS`, default 4) over a prerequisite
DAG; use `ci-fast-core` / `release-gate-core` for a true `-j1` sequential
diagnosis of the same graph. Coverage shards write distinct `COVERAGE_FILE`s
under the release coverage runtime and combine once; subprocess pools are
capped with `RELEASE_PYTEST_WORKERS`, `DBT_TEST_WORKERS`, and
`MUTMUT_MAX_CHILDREN`. GitHub's automatic `tests` worker runs the parallel fast
suite and serial `make contract-http` while independent static/docs and dbt-lint
workers run in parallel. A required Python 3.13 worker repeats package smoke and
the ordinary suite while Python 3.10 remains the supported floor and full-release
runtime. The `contract` marker remains excluded from `make test` and
`make test-cov`.

Dagster integration is layered:

1. `make dagster-jobs-smoke` — every registered public job with mocked externals.
2. Wiring tests — recording/fake dbt resource asserts select/exclude for shipped
   scoped jobs.
3. One real disposable-DuckDB/dbt end-to-end materialization per shipped scope,
   plus focused writer recovery. Incremental/full-refresh equivalence and golden
   mart semantics stay in the dbt integration suite.

Together with the other coverage commands, they enforce 100% branch coverage for
`src/oddsfox_pipeline` except the warehouse profiling operator helpers under
`storage/duckdb/profile/`, which are covered by smoke tests instead.

`make data-quality` rebuilds disposable dbt state and runs the dbt-native model
and data tests. `make mutation` resumes cached focused Mutmut work; `make
mutation-ci` deletes `mutants/` first and is the deterministic release gate.
Its five-module scope covers outbound URL safety, raw snapshot contracts,
market-scope predicates, market persistence, and odds planning. Mutation output
is local or a short-lived Manual Full Validation artifact and must not be
committed.

The dbt integration suite requires incremental/full-refresh equivalence for
every incremental odds model, including late, null-refresh, new-key, uniqueness,
and retention cases.

When `.env` sets `DUCKDB_PATH`, use `isolate_duckdb_test_env()` from
`tests/unit/storage/duckdb_storage_test_support.py` so tests do not write to the
production warehouse. See
[Development](../docs/development/index.md#local-env-and-tests).
