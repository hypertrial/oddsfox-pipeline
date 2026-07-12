# AGENTS.md

OddsFox Pipeline is an open-source, local-first prediction-market data pipeline.
Version `0.1.x` ships a WC2026 Polymarket pipeline for FIFA World Cup
2026 markets and odds, a Kalshi WC2026 pipeline for stage and group-winner
markets, plus a small FIFA fixture/results source for real-team
scope validation.
Stack: **Dagster** (orchestration), **dlt** (market landing), **dbt** +
**DuckDB** (warehouse/analytics), **uv** (deps), **Ruff** + **sqlfluff**
(lint), **pytest** (tests).

## Setup

```bash
uv sync --extra dev
cp .env.example .env
```

Default warehouse: `oddsfox.duckdb` in the repo root. Keep schedules disabled in local dev and CI unless intentionally running live ingestion:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

## AI agent guidance

Cursor loads [Ponytail](https://github.com/DietrichGebert/ponytail) from [`.cursor/rules/ponytail.mdc`](.cursor/rules/ponytail.mdc) (`alwaysApply: true`). It complements this file: reuse existing code, prefer stdlib and installed deps, and keep diffs minimal. Do not cut validation, error handling, security, or tests that cover real behavior. Mark intentional shortcuts with a `ponytail:` comment (name the ceiling and upgrade path).

Other agents should read this `AGENTS.md` at the repo root.

## No legacy support (v0.1.x)

OddsFox Pipeline is v0.1.x — too new for a supported legacy surface, migration path, or
backward-compatibility layer unless the task explicitly requests one.

- **Remove and replace** old APIs, config values, warehouse layouts, and marts;
  do not add adapters, aliases, deprecation periods, or dual code paths.
- **Warehouse reset over migration:** operators with pre-layout DuckDB files
  should delete the warehouse (`rm oddsfox.duckdb*`) and rerun quickstart.
- **Public contracts:** [Data contracts](docs/reference/data-contracts.md) marts
  and Dagster asset keys are the current API. Breaking changes belong in
  [CHANGELOG.md](CHANGELOG.md), not hidden compat layers.
- **Ponytail alignment:** deletion over addition applies here — prefer removing
  dead paths over preserving them.

Do not add `legacy`, `compat`, `deprecated`, or `migration` code paths unless
the user explicitly asks. Do not preserve removed config values (e.g.
`wc2026_legacy`) or reintroduce removed marts/APIs. Do not document long-term
semver stability; v0.1.x may break between releases.

## Quality gate (run before finishing work)

Mirrors [`.github/workflows/ci.yml`](.github/workflows/ci.yml). Use `uv run make …` from the repo root.

```bash
uv run make lint
uv run make test-cov
uv run make dagster-jobs-smoke-cov
uv run make dagster-refresh-cov
uv run make integration-dbt-cov
uv run make dbt-unit
uv run make golden-dbt
uv run make dbt-source-freshness-ci
uv run make coverage-report
uv run make docs-check
uv run make check-secrets
uv run make dbt-parse
uv run make dbt-build-ci
uv run make gx-data-quality
uv run make costguard
```

CI runs these checks as parallel jobs and uses a docs-only fast lane for
changes limited to README/top-level docs, `docs/**`, `mkdocs.yml`, and project
policy docs. For local one-shot runs, `make test`, `make integration-dagster`,
`make integration-dbt`, `make data-quality`, and `make coverage` still work
without the CI coverage-accumulation split.

`dbt-build-ci` bootstraps a disposable DuckDB database under `.cache/` before
running dbt build. CI then calls `gx-data-quality` against that existing
disposable database so data-quality checks do not rebuild dbt. `contract-http`
is replay-only and manual/nightly; it is not part of the default CI gate.
Costguard is a dbt/CI guardrail, not an odds ingestion runtime dependency.
Install the pinned local scanner with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

### Targeted commands

| Target | Purpose |
|--------|---------|
| `make format` | Ruff format + dbt parse + sqlfluff fix |
| `make unit-core` | Config, resources, storage unit tests |
| `make unit-ingest` | Ingestion unit tests |
| `make unit-orchestration` | Orchestration/Dagster unit tests |
| `make dagster-jobs-smoke` | Headless smoke for every registered public Dagster job with mocked externals |
| `make dagster-jobs-smoke-cov` | CI coverage version of registered public Dagster job smoke |
| `make dagster-refresh-cov` | CI coverage for deeper seeded Dagster refresh-path smoke tests |
| `make test-cov` | CI unit tests with coverage accumulation (`-n auto`) |
| `make integration-dbt` | DuckDB + dbt integration smoke |
| `make integration-dagster` | Dagster integration smoke |
| `make integration-dbt-cov` | CI DuckDB + dbt integration with coverage append |
| `make integration-dagster-cov` | Local wrapper for both split CI Dagster coverage targets |
| `make dbt-unit` | dbt unit tests for high-risk SQL branches |
| `make golden-dbt` | Exact-output dbt mart regression fixtures |
| `make dbt-source-freshness-ci` | Seed fresh temp source rows and run dbt source freshness |
| `make coverage-report` | CI coverage report gate (`--fail-under=100`) |
| `make check-secrets` | Repo policy check for tracked secret leakage |
| `make dbt-build-ci` | Bootstrap disposable DuckDB + dbt build |
| `make gx-data-quality` | Great Expectations data-quality report against an existing disposable dbt build |
| `make data-quality` | Safe local wrapper that rebuilds disposable dbt state before `gx-data-quality` |
| `make contract-http` | Replay-only HTTP contract tests; manual/nightly, excluded from default gates |
| `make costguard-scan` | Run the dbt cost guardrail against an existing dbt build |
| `make costguard` | Safe local wrapper that rebuilds disposable dbt state before Costguard |
| `make dagster-dev` | Local Dagster UI |
| `make docs-serve` | MkDocs dev server |

## Project layout

```
.cursor/rules/     # Cursor agent rules (ponytail)
src/oddsfox_pipeline/
  config/          # Settings barrel (settings.py re-exports warehouse + polymarket)
  ingestion/       # dlt sources, Polymarket fetch/sync/backfill, odds engine
  orchestration/   # Dagster assets, jobs, schedules, dbt wiring
  resources/       # HTTP, outbound URL, progress guardrails
  storage/duckdb/  # Connection, schemas, markets/odds persistence, profiling
dbt/
  models/international_results_wc2026/{staging,intermediate,marts,observability}/
  models/polymarket_wc2026/{staging,intermediate,marts,observability}/
  models/polymarket_us_midterms_2026/{staging,intermediate,marts,observability}/
  models/kalshi_wc2026/{staging,intermediate,marts,observability}/
  tests/           # Singular dbt data tests (assert_*)
tests/
  unit/            # Mocked config, ingestion, storage, orchestration
  integration/     # DuckDB/dbt/Dagster smoke (temp databases)
  dbt/             # dbt project structure checks
docs/              # Project docs (MkDocs)
scripts/           # Warehouse audits, repairs, profiling (not CI gate)
```

Imports use src-layout paths: `from oddsfox_pipeline.config.settings import …`, not relative imports across package boundaries.

## Code style

**Python** ([pyproject.toml](pyproject.toml)):

- Ruff: `target-version = "py310"`, line length 88, double quotes, isort (`extend-select = ["I"]`).
- Run `make format` before committing Python changes; `make lint` checks format + ruff check.
- Match existing module boundaries; avoid drive-by refactors outside the task scope.

**dbt SQL**:

- sqlfluff: DuckDB dialect, dbt templater, max line length 130.
- Lint/fix: `dbt/models`, `dbt/tests` only (see Makefile).
- Layer naming: source-first schemas such as `polymarket_wc2026_staging`,
  `polymarket_wc2026_marts`, `polymarket_us_midterms_2026_marts`,
  `kalshi_wc2026_marts`, and `international_results_wc2026_marts`.

**Tests** ([tests/README.md](tests/README.md)):

- Default `make test` excludes `integration`, `performance`, `slow`, `repo_check`.
- Default `make test` and `make test-cov` also exclude `contract` replay tests.
- Mark slow/external tests with the appropriate pytest marker; do not widen default test scope without reason.
- Add or update tests for behavior changes in the matching `tests/unit/` or `tests/integration/` subtree.

## Orchestration guardrails

Asset key order (routine pipeline; flat op names use the same subject order):

1. `polymarket/wc2026/raw/markets`
2. `polymarket/wc2026/raw/markets_snapshot`
3. `polymarket/wc2026/ops/market_scope_registry`
4. `polymarket/wc2026/raw/market_metadata_backfill`
5. `polymarket/wc2026/raw/token_odds_history_hourly`
6. `polymarket/us_midterms_2026/raw/markets`
7. `polymarket/us_midterms_2026/raw/markets_snapshot`
8. `polymarket/us_midterms_2026/ops/market_scope_registry`
9. `polymarket/us_midterms_2026/raw/market_metadata_backfill`
10. `polymarket/us_midterms_2026/raw/token_odds_history_hourly`
11. `international_results/wc2026/raw/match_results`
12. `kalshi/wc2026/raw/events` (dlt sibling landed with markets)
13. `kalshi/wc2026/raw/markets`
14. `kalshi/wc2026/raw/markets_snapshot`
15. `kalshi/wc2026/ops/market_scope_registry`
16. `kalshi/wc2026/raw/market_candlesticks_hourly`
17. dbt model assets under `polymarket/wc2026/{staging,intermediate,marts,observability}/...`,
   `polymarket/us_midterms_2026/{staging,intermediate,marts,observability}/...`,
   `international_results/wc2026/{staging,intermediate,marts,observability}/...`,
   and `kalshi/wc2026/{staging,intermediate,marts,observability}/...`

Key jobs: `international_results_wc2026_match_results_ingest`,
`polymarket_wc2026_market_registry_refresh`, `polymarket_wc2026_hourly_odds_ingest`,
`polymarket_wc2026_dbt_build`, `polymarket_wc2026_full_pipeline`,
`polymarket_us_midterms_2026_market_registry_refresh`,
`polymarket_us_midterms_2026_hourly_odds_ingest`,
`polymarket_us_midterms_2026_dbt_build`,
`polymarket_us_midterms_2026_full_pipeline`,
`kalshi_wc2026_market_registry_refresh`, `kalshi_wc2026_hourly_odds_ingest`,
`kalshi_wc2026_dbt_build`, `kalshi_wc2026_full_pipeline`.

Schedules target `polymarket_wc2026_hourly_odds_ingest`,
`polymarket_us_midterms_2026_hourly_odds_ingest`, and
`kalshi_wc2026_hourly_odds_ingest`; all are **stopped by default**.
Do not enable live/hourly schedules in code or `.env` unless the task explicitly requires it.

**Market scope:** v0.1.x ships fixed Dagster/dbt graphs for `wc2026` and
`us_midterms_2026` on Polymarket plus `wc2026` on Kalshi. Polymarket scope
helpers may load other slug-like seed entries for tests and future work, but
Dagster asset configs do not accept a runtime scope selector. See
[Configuration](docs/reference/configuration.md).

**Kalshi env vars:** `KALSHI_REQUESTS_PER_SECOND`,
`KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`. Kalshi uses the public trade API;
no API credentials are required for local docs, dbt, or mocked tests.

DuckDB is local-only runtime state. For read-only inspection prefer `scripts/profile_warehouse.py` over opening the warehouse read-write.

## Do not

- Commit `.env`, secrets, `*.duckdb` / WAL/SHM files, parquet/CSV exports, or other local artifacts (see [`.gitignore`](.gitignore)).
- Invent commands outside the Makefile; if a check is missing, add a Makefile target rather than documenting one-off scripts as the gate.
- Add runtime scope such as soccer context, simulations, allocation, or web integration without explicit product direction; v0.1.x ships the WC2026 Polymarket ingest and warehouse implementation only.
- Add legacy, compat, deprecated, or migration shims unless the task explicitly requests backward compatibility.

## Pull requests

1. Branch from `main`.
2. Keep changes focused; one concern per PR when possible.
3. Ensure the quality gate passes locally.
4. Update docs in `docs/` when behavior, configuration, or project positioning changes.

## Further reading

- [OddsFox Pipeline docs](docs/index.md) — overview, runbooks, warehouse, troubleshooting
- [CONTRIBUTING.md](CONTRIBUTING.md) — contributor workflow and CI parity
- [Orchestration](docs/reference/orchestration.md) — assets, jobs, and schedules
- [Configuration](docs/reference/configuration.md) — `.env` reference
