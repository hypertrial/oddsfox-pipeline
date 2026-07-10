# Development

Use this page when changing code, dbt models, docs, or orchestration behavior.
OddsFox is a prediction-market pipeline; v0.1.x development touches the WC2026 and
US midterms 2026 Polymarket adapters, marts, and orchestration. For operator
setup, start with [Quickstart](quickstart.md).

## Repo Layout

| Path | Purpose |
| --- | --- |
| `src/oddsfox_pipeline` | Python package for config, ingestion, storage, resources, and orchestration. |
| `dbt` | DuckDB dbt project, profiles, macros, models, and data tests. |
| `docs` | MkDocs site content and OddsFox dark CSS. |
| `scripts` | Operator utilities for warehouse inspection, compaction, pruning, repair, and WC2026 exports. |
| `tests` | Unit, integration, dbt, Dagster, and repo policy tests. |

## Local Setup

```bash
uv sync --extra dev
cp .env.example .env
```

Keep schedules disabled for local development unless intentionally testing live
ingestion:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

## Quality Gate

Run the same checks as CI. The canonical command list lives in
[AGENTS.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/AGENTS.md)
and [CONTRIBUTING.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CONTRIBUTING.md#quality-gate).

`dagster-jobs-smoke` executes every registered public Dagster job with
deterministic temp resources, mocked external APIs, and no `dagster-dev`
server. `integration-dagster` includes that check plus deeper seeded Dagster
smokes. `coverage` enforces 100% branch coverage for product-core package code;
warehouse profiling helpers are smoke-tested instead. These gates run in GitHub
Actions and should pass locally before opening a pull request.

Costguard medium/low findings are measured dbt debt, not automatic
materialization work. Before changing dbt materializations or adding
incremental models, capture the failing advisory, dbt build runtime, and
warehouse/profile size evidence that justifies the change.

## dbt Materialization Debt

Treat Costguard medium/low advisories as measurement prompts. Current measured
debt includes:

- `int_polymarket_wc2026_token_universe` is materialized as a table because profiling
  showed it is reused heavily by WC2026 marts and the dbt build stayed
  neutral or faster after the change.
- `int_polymarket_wc2026_market_tokens` is materialized as a table because it
  feeds the public knockout token classifier. Costguard now tracks its remaining
  incremental-conversion question as `SQLCOST040`.
- `int_polymarket_wc2026_token_hourly_odds` is an incremental private fact that
  reprocesses dirty hourly buckets from raw odds `ingested_at` overlap.
- `SQLCOST040`: `int_polymarket_wc2026_token_universe` and
  `int_polymarket_wc2026_market_tokens` still track remaining materialization
  questions. Keep collecting row-volume profiling before further conversions.
- Low advisories may still flag `ORDER BY` without `LIMIT` in table-building
  marts; treat them as profiling prompts, not automatic refactors.

Do not change materializations on advisory text alone. Capture dbt build
runtime, relevant relation sizes from `scripts/profile_warehouse.py`, and the
Costguard finding before switching a model to table or incremental.

## Adding A Scope

OddsFox v0.1.x ships fixed scopes, not a runtime scope selector. Add a scope by
making the static surfaces explicit and letting the guard tests catch drift:

1. Add the source discovery seed entry, for example in the Polymarket or Kalshi
   `market_scopes.yml`.
2. Add a `ScopeSpec` in `oddsfox_pipeline.orchestration.scope_registry` with the
   source/scope ref, namespace alias, fixed Dagster jobs, and dbt selector.
3. Add explicit Dagster assets/jobs in the source module; keep asset keys and op
   names source-first and scope-first.
4. Add dbt source YAML, model folder tags, and a contract seed when the scope
   ships analytics.
5. Update quickstart, operations, scripts, and this checklist when operator
   behavior changes.
6. Run the registry, dbt-structure, orchestration, and docs tests before the
   broader quality gate.

## Local `.env` And Tests

`DUCKDB_PATH` in `.env` overrides `DUCKDB_NAME` and can leak into unit tests
when settings reload from disk. See [Configuration](configuration.md#local-development)
and [Troubleshooting](troubleshooting.md#tests-writing-to-production-warehouse).

- Use the shared `duck` fixture from `tests/unit/storage/duckdb_storage_test_support.py`
  for storage tests that need a disposable warehouse.
- Call `isolate_duckdb_test_env(monkeypatch, db_path)` in ingestion or
  orchestration tests that reload settings but cannot use the `duck` fixture
  directly.

## Targeted Test Commands

| Target | Use |
| --- | --- |
| `uv run make unit-core` | Config, resource, and storage unit tests. |
| `uv run make unit-ingest` | Polymarket ingestion and odds sync tests. |
| `uv run make unit-orchestration` | Dagster asset, job, and schedule tests. |
| `uv run make dagster-jobs-smoke` | Headless deterministic smoke for every registered public Dagster job. |
| `uv run make integration-dbt` | DuckDB and dbt smoke tests. |
| `uv run make integration-dagster` | Dagster integration smoke tests. |
| `uv run make test-cov` | CI unit tests with coverage accumulation (`-n auto`). |
| `uv run make integration-dagster-cov` | CI Dagster integration with coverage append. |
| `uv run make integration-dbt-cov` | CI DuckDB + dbt integration with coverage append. |
| `uv run make coverage-report` | CI coverage report gate (`--fail-under=100`). |
| `uv run make dbt-build-ci` | Bootstrap disposable DuckDB and run dbt build. |
| `uv run make costguard` | Run the pinned dbt cost guardrail locally. |
| `uv run make coverage` | Local one-shot 100% product-core branch coverage gate. |

## Pull Request Expectations

- Keep PRs focused and update docs for behavior or operator workflow changes.
- Breaking changes are OK in v0.1.x; document them in CHANGELOG and data
  contracts — do not add legacy fallbacks unless the PR explicitly scopes compat
  work.
- Add or update tests for changed behavior.
- Do not commit `.env`, local DuckDB files, generated dbt targets, `site/`, or
  data exports.
- Follow [CONTRIBUTING](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CONTRIBUTING.md)
  for the full contribution workflow.
