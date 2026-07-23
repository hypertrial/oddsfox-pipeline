# Development

Use this page when changing code, dbt models, docs, or orchestration behavior.
OddsFox Pipeline is a prediction-market pipeline; v0.1.x development touches the WC2026 and
US midterms 2026 Polymarket adapters, marts, and orchestration. For operator
setup, start with [Quickstart](../getting-started/index.md).

## Repo Layout

| Path | Purpose |
| --- | --- |
| `src/oddsfox_pipeline` | Python package for config, ingestion, storage, resources, and orchestration. |
| `dbt` | DuckDB dbt project, profiles, macros, models, and data tests. |
| `docs` | MkDocs content, self-hosted fonts, and the small OddsFox Pipeline theme extension. |
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

For documentation work, install Chromium once and leave the live-reload server
running while you edit:

```bash
uv run playwright install chromium
uv run make docs-serve
```

Open `http://127.0.0.1:8000`. MkDocs rebuilds and refreshes the page after each
saved documentation, stylesheet, or configuration change; no restart is needed.

## Quality Gate

Run the full local release checks. The canonical command list lives in
[AGENTS.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/AGENTS.md)
and [CONTRIBUTING.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CONTRIBUTING.md#quality-gate).

`dagster-jobs-smoke` executes every registered public Dagster job with
deterministic temp resources, mocked external APIs, and no `dagster-dev`
server. `integration-dagster` includes that check plus deeper seeded Dagster
smokes. `coverage` enforces 100% branch coverage for product-core package code;
warehouse profiling helpers are smoke-tested instead. GitHub Actions uses one
runner for less than five cumulative minutes and runs a compact offline gate:
lint, fast tests, saved HTTP contracts, dbt parse, and a strict docs build.
Run the complete gate locally before a release.

The deterministic trust checks are separate from live ingestion. `dbt-unit`
exercises high-risk SQL branches with dbt unit tests, `golden-dbt` compares
public mart rows against exact fixtures, `dbt-source-freshness-ci` seeds current
loaded-at rows before `dbt source freshness`, and `gx-data-quality` writes a
local Great Expectations report under `.cache/` after `dbt-build-ci` has already
created the disposable warehouse. `data-quality` remains the safe local wrapper
that rebuilds first. `contract-http` replays sanitized HTTP cassettes in the
fast GitHub gate; the default `make test` command excludes the `contract` marker.
`live-smoke` is also opt-in: it runs the combined WC2026 public-source job with
a smoke-only 24-hour odds window, no historical backfill, and the normal
Polymarket volume floor. Production job defaults are unchanged. Live ingestion
is local-only and must not run in GitHub Actions.
`match-minute-live-smoke` is the disposable, opt-in acceptance smoke for the
completed-match minute backfill. It rebuilds `.cache/match_minute_live_smoke.duckdb`
and fails unless the quality model reports exactly 104 games, 248 markets (216
group and 32 knockout), 496 tokens, one valid results revision/hash, 496
successful published fetch audits, zero structural issue rows, and no blocking
issue. Warning counts are intentionally not pinned.
`dbt-polygon-settlement-ci` is the network-free gate for the isolated Polygon
graph: it creates complete synthetic seed/scan/chunk/fill fixtures, runs only
`tag:polygon_settlement`, and asserts the exact 39,120-row mart. The ordinary
`dbt-build` excludes that tag. `polygon-settlement-live-smoke` is separately
opt-in and requires a finalized-capable Polygon RPC; neither Polygon job has a
schedule.

Dagster dbt assets enable dbt source tests as asset checks. Row-count and
column metadata fetching is available through `DbtBuildConfig` but stays
opt-in because DuckDB in-process integration tests share local database
connections.

Costguard high findings must be fixed or justified with an inline suppression
and dbt grain tests that prove the intended shape. Medium/low findings are
measured dbt debt, not automatic materialization work. Before changing dbt
materializations or adding incremental models, capture the failing advisory,
dbt build runtime, and warehouse/profile size evidence that justifies the
change.

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
- Remaining medium/low Costguard advisories are known dbt debt and do not make
  the gate fail while the scanner exits successfully.

Do not change materializations on advisory text alone. Capture dbt build
runtime, relevant relation sizes from `scripts/profile_warehouse.py`, and the
Costguard finding before switching a model to table or incremental.

## Adding A Scope

OddsFox Pipeline v0.1.x ships fixed scopes, not a runtime scope selector. Add a scope by
making the static surfaces explicit and letting the guard tests catch drift:

1. Add the source discovery seed entry, for example in the Polymarket or Kalshi
   `market_scopes.yml`.
2. Add a `ScopeSpec` in `oddsfox_pipeline.orchestration.scope_registry` with the
   source/scope ref, namespace alias, fixed Dagster jobs, and dbt selector.
3. Add explicit Dagster assets/jobs in the source module; keep asset keys and op
   names source-first and scope-first.
4. Add dbt source YAML, model folder tags, and a contract seed when the scope
   ships analytics.
5. Update the quickstart, scope guide, orchestration reference, scripts, and
   this checklist when operator behavior changes.
6. Run the registry, dbt-structure, orchestration, and docs tests before the
   broader quality gate.

## Local `.env` And Tests

`DUCKDB_PATH` in `.env` overrides `DUCKDB_NAME` and can leak into unit tests
when settings reload from disk. See
[Configuration](../reference/configuration.md#local-development) and
[Troubleshooting](../guides/troubleshooting.md#tests-writing-to-production-warehouse).

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
| `uv run make dagster-jobs-smoke-cov` | Coverage version of registered public Dagster job smoke. |
| `uv run make dagster-refresh-cov` | Coverage for deeper seeded Dagster refresh-path smoke tests. |
| `uv run make integration-dbt` | DuckDB and dbt smoke tests. |
| `uv run make integration-dagster` | Dagster integration smoke tests. |
| `uv run make test-cov` | Unit tests with coverage accumulation (`-n auto`). |
| `uv run make integration-dagster-cov` | Local wrapper for both split Dagster coverage targets. |
| `uv run make integration-dbt-cov` | DuckDB + dbt integration with coverage append. |
| `uv run make dbt-unit` | dbt unit tests for classifier and mart edge-case SQL. |
| `uv run make golden-dbt` | Exact-output public mart regression fixtures. |
| `uv run make dbt-source-freshness-ci` | Seed temp source rows and run dbt source freshness. |
| `uv run make coverage-report` | Coverage report gate (`--fail-under=100`). |
| `uv run make check-secrets` | Repo policy check for tracked secret leakage. |
| `uv run make dbt-build-ci` | Bootstrap disposable DuckDB and run dbt build. |
| `uv run make dbt-polygon-settlement-ci` | Build the isolated Polygon settlement graph against replay fixtures. |
| `uv run make gx-data-quality` | Great Expectations-style report against an existing disposable dbt build. |
| `uv run make data-quality` | Safe local wrapper that rebuilds disposable dbt state before `gx-data-quality`. |
| `uv run make contract-http` | Replay-only HTTP contract tests; included in the fast GitHub gate. |
| `uv run make live-smoke` | Opt-in live WC2026 cross-platform pipeline. |
| `uv run make match-minute-live-smoke` | Opt-in disposable live acceptance check for the 104-game Polymarket minute mart. |
| `uv run make polygon-settlement-live-smoke` | Opt-in finalized Polygon backfill against a disposable warehouse. |
| `uv run make polygon-settlement-seed-validate` | Validate the reviewed 248-proposition static manifest. |
| `uv run make costguard-scan` | Run the pinned dbt cost guardrail against an existing dbt build. |
| `uv run make costguard` | Safe local wrapper that rebuilds disposable dbt state before Costguard. |
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
