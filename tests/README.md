# tests

This subtree validates OddsFox Pipeline. Version `0.2.x`
ships WC2026 Polymarket and Kalshi ingestion, marts, and orchestration.

See [OddsFox Pipeline docs](../docs/index.md) for setup and runbook commands.
Targeted Make commands and quality gates live in [AGENTS.md](../AGENTS.md).

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

Dagster integration is layered:

1. `make dagster-jobs-smoke` — every registered public job with mocked externals.
2. Wiring tests — recording/fake dbt resource asserts select/exclude for shipped
   scoped jobs.
3. One real disposable-DuckDB/dbt end-to-end materialization per shipped scope,
   plus focused writer recovery. Incremental/full-refresh equivalence and golden
   mart semantics stay in the dbt integration suite.

`make pipelines-deterministic` composes the Dagster integration suite,
`integration-dbt`, `dbt-polygon-settlement-ci`, `dbt-match-minute-ci`, and
`dbt-build-ci` for offline validation of all six product pipelines. It is a
dev-only convenience target, not a `ci-fast` substitute.

Together with the other coverage commands, they enforce 100% branch coverage for
`src/oddsfox_pipeline` except the warehouse profiling operator helpers under
`storage/duckdb/profile/`, which are covered by smoke tests instead.

The dbt integration suite requires incremental/full-refresh equivalence for
every incremental odds model, including late, null-refresh, new-key, uniqueness,
and retention cases.

When `.env` sets `DUCKDB_PATH`, use `isolate_duckdb_test_env()` from
`tests/unit/storage/duckdb_storage_test_support.py` so tests do not write to the
production warehouse. See
[Development](../docs/development/index.md#local-env-and-tests).
