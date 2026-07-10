# Operations

Use this page when running Dagster assets, jobs, schedules, or recovery paths.
For data outputs, see [Warehouse](warehouse.md) and
[Data Contracts](data-contracts.md).

The v0.1.x orchestration surface is WC2026 Polymarket, Kalshi WC2026, US
midterms 2026 Polymarket, plus a small FIFA World Cup fixture/result source.

## Dagster Assets

The main asset key order is:

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

Flat Dagster op names remain source-first, for example
`polymarket_wc2026_raw_token_odds_history_hourly`.

## Jobs

- `polymarket_wc2026_market_registry_refresh`: WC2026 market discovery, registry refresh, and metadata backfill.
- `polymarket_wc2026_hourly_odds_ingest`: hourly WC2026 token odds refresh (trailing 30 days by default).
- `international_results_wc2026_match_results_ingest`: WC2026 FIFA World Cup fixture/result CSV refresh.
- `polymarket_wc2026_dbt_build`: dbt analytics build for the WC2026 mart surface, including knockout marts.
- `polymarket_wc2026_full_pipeline`: WC2026 result refresh, market discovery, hourly odds refresh (trailing 30 days), and dbt analytics build.
- `polymarket_us_midterms_2026_market_registry_refresh`: targeted US midterms 2026 market discovery, registry refresh, and metadata backfill.
- `polymarket_us_midterms_2026_hourly_odds_ingest`: hourly US midterms 2026 token odds refresh (trailing 30 days by default).
- `polymarket_us_midterms_2026_dbt_build`: scoped US midterms dbt build (`tag:us_midterms_2026`).
- `polymarket_us_midterms_2026_full_pipeline`: US midterms market discovery, hourly odds refresh, and scoped dbt build (`tag:us_midterms_2026` only; no WC2026 or FIFA results assets).
- `kalshi_wc2026_market_registry_refresh`: Kalshi WC2026 series discovery and registry refresh.
- `kalshi_wc2026_hourly_odds_ingest`: hourly Kalshi candlestick refresh for admitted registry markets.
- `kalshi_wc2026_dbt_build`: scoped Kalshi dbt build (`+tag:kalshi`, excluding cross-domain Polymarket tests).
- `kalshi_wc2026_full_pipeline`: FIFA results refresh, Kalshi market discovery, hourly candlestick refresh, and scoped dbt build (`+tag:kalshi`, including `international_results` parents; excludes `tag:cross_domain` and `tag:polymarket` tests outside that closure).

## Headless Job Smoke

To verify that every registered public Dagster job can execute without starting
`dagster-dev`, run:

```bash
uv run make dagster-jobs-smoke
```

This deterministic smoke uses temp DuckDB state and mocked external APIs. It
checks Dagster job registration, asset selection, run config, and resource
wiring. It is not a live ingestion run.

## Deterministic Validation

Use these checks when you need CI-like confidence without live APIs or
`dagster-dev`:

```bash
uv run make dbt-unit
uv run make golden-dbt
uv run make dbt-source-freshness-ci
uv run make data-quality
```

`dbt-unit` covers SQL branch behavior, `golden-dbt` compares exact public mart
fixture rows, `dbt-source-freshness-ci` seeds current source timestamps before
running dbt freshness, and `data-quality` writes local Great Expectations
report artifacts under `.cache/`. `contract-http` is replay-only and manual or
nightly; it is not part of the default deterministic gate.

## Run A Scope

Use `scripts/run_scope.py` for local CLI runs. It maps known scope refs to fixed
Dagster jobs; it is not a runtime market-scope selector and does not accept
arbitrary dbt selectors. Unlike `dagster-jobs-smoke`, this command executes the
real fixed jobs and may call configured external sources.

```bash
uv run python scripts/run_scope.py --list
uv run python scripts/run_scope.py polymarket:wc2026 --step full
uv run python scripts/run_scope.py polymarket:wc2026 kalshi:wc2026 --step dbt
uv run python scripts/run_scope.py polymarket_us_midterms_2026 --step odds --dry-run
```

Supported refs are `polymarket:wc2026` (`polymarket_wc2026`),
`polymarket:us_midterms_2026` (`polymarket_us_midterms_2026`), and
`kalshi:wc2026` (`kalshi_wc2026`). Supported steps are `registry`, `odds`,
`dbt`, and `full`.

For WC2026 scopes, `full` refreshes `international_results_wc2026_match_results_ingest`
before dbt. If you run staged `dbt` steps manually, refresh that results job
first so real-team validation inputs are current.

The direct Dagster equivalent remains available:

```bash
.venv/bin/python -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j polymarket_wc2026_full_pipeline
```

## Polymarket scopes

The shipped Dagster jobs and dbt graphs are fixed per scope (`wc2026`,
`us_midterms_2026`).

### WC2026

- `polymarket/wc2026/raw/markets` performs the single Gamma market discovery pass,
  lands raw market rows through dlt, and persists token mappings from the same
  payload after market landing succeeds.
- `polymarket/wc2026/raw/markets_snapshot` records a local raw-layer snapshot
  for lineage/accounting and does not call Gamma.
- `polymarket/wc2026/ops/market_scope_registry` refreshes
  `polymarket_wc2026_ops.market_scope_registry` only when the preceding market
  discovery did not already refresh the registry.
- `polymarket/wc2026/raw/market_metadata_backfill` and
  `polymarket/wc2026/raw/token_odds_history_hourly` run over the fixed WC2026
  registry.
- `international_results/wc2026/raw/match_results` loads only FIFA World Cup rows
  inside the 2026 tournament window and feeds real-team validation in dbt.
- dbt model assets under `polymarket/wc2026/...` and
  `international_results/wc2026/...` build the fixed WC2026 dbt graph.

### US midterms 2026

- `polymarket/us_midterms_2026/raw/markets` uses targeted discovery for Balance
  of Power, Senate control, and House control event slugs only.
- `polymarket/us_midterms_2026/ops/market_scope_registry` and downstream odds
  assets mirror the WC2026 raw/ops flow in a parallel namespace.
- There is no results ingestion or candidate/race validation layer for this scope in v1.
- dbt model assets under `polymarket/us_midterms_2026/...` build a simple
  markets + hourly-odds mart without office-type classification.

### Kalshi WC2026

- `kalshi/wc2026/raw/markets` performs the Kalshi series/event/market discovery
  pass and lands raw events and markets through dlt.
- `kalshi/wc2026/raw/markets_snapshot` records a local raw-layer snapshot for
  lineage/accounting and does not call Kalshi.
- `kalshi/wc2026/ops/market_scope_registry` refreshes
  `kalshi_wc2026_ops.market_scope_registry` when the preceding market discovery
  did not already refresh the registry.
- `kalshi/wc2026/raw/market_candlesticks_hourly` syncs hourly candlesticks for
  admitted registry markets into `kalshi_wc2026_raw.market_candlesticks_hourly`.
- dbt model assets under `kalshi/wc2026/...` build the fixed Kalshi WC2026 dbt
  graph. Kalshi uses the public trade API; no credentials are required.

## Schedules

Schedules are stopped by default.

- `polymarket_wc2026_hourly_odds_schedule`: every hour for `polymarket_wc2026_hourly_odds_ingest` (`fidelity=60`).
- `polymarket_us_midterms_2026_hourly_odds_schedule`: every hour for `polymarket_us_midterms_2026_hourly_odds_ingest` (`fidelity=60`).
- `kalshi_wc2026_hourly_odds_schedule`: every hour for `kalshi_wc2026_hourly_odds_ingest` (`fidelity=60`).

Enable only after manual jobs are healthy:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

## Recovery

- Re-run `polymarket_wc2026_hourly_odds_ingest` for routine WC2026 odds gaps.
- Re-run `polymarket_us_midterms_2026_hourly_odds_ingest` for routine US midterms
  odds gaps.
- Re-run `kalshi_wc2026_hourly_odds_ingest` for routine Kalshi candlestick gaps.
- Re-run `international_results_wc2026_match_results_ingest` when the source CSV
  updates completed scores or fixtures.
- Run `polymarket_wc2026_dbt_build` after WC2026 raw or ops table repairs.
- Run `polymarket_us_midterms_2026_dbt_build` after US midterms raw or ops table repairs.
- Run `kalshi_wc2026_dbt_build` after Kalshi raw or ops table repairs.
- Prune old `polymarket_wc2026_raw.odds_history` rows with `make prune-odds-history` (default 365-day retention; use `--dry-run` on the script to preview). The script targets WC2026 raw odds only.
- Reclaim DuckDB file dead space with `make compact-warehouse` after pruning or full refreshes.
- Use `scripts/profile_warehouse.py` to inspect relation counts and freshness without opening the database read-write.

## Post-run validation (US midterms)

After a successful midterms pipeline run, spot-check the warehouse:

```sql
-- Registry coverage (expect ~20–25 markets across three event slugs)
SELECT count(*) AS registry_markets
FROM polymarket_us_midterms_2026_ops.market_scope_registry;

-- Admitted public mart markets (volume floor excludes zero-volume placeholders)
SELECT count(DISTINCT market_id) AS mart_markets
FROM polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds;

-- Latest hourly odds timestamp (trailing 30-day contract window)
SELECT max(odds_hour_epoch) AS latest_hour
FROM polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds;

-- Run telemetry (populated after market discovery and odds sync jobs)
SELECT count(*) AS pipeline_events
FROM polymarket_us_midterms_2026_ops.pipeline_run_events;

SELECT count(*) AS observability_rows
FROM polymarket_us_midterms_2026_observability.polymarket_us_midterms_2026_sync_run_observability;
```

Confirm `dbt build --select tag:us_midterms_2026` reports all models and tests
passing.

## Landing And Finalization

Canonical raw and ops table schemas remain stable for operators and dbt. dlt
lands markets, odds-history batches, WC2026 registry batches, and pipeline
run-event batches; dlt stage tables and `_dlt*` metadata tables are internal.
Raw market-token mappings are extracted from the same Gamma payload as markets
and finalized through the canonical DuckDB helper.

Scheduler ledger rows, skip state, and daily odds aggregates remain custom SQL
finalizers because they preserve monotonic cursors, scheduler state, first-seen
skip timestamps, and aggregate rebuild semantics.
