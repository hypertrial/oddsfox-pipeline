# Operations

Use this page when running Dagster assets, jobs, schedules, or recovery paths.
For data outputs, see [Warehouse](warehouse.md) and
[Data Contracts](data-contracts.md).

The v0.1.x orchestration surface is WC2026 Polymarket, US midterms 2026
Polymarket, plus a small FIFA World Cup fixture/result source.

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
12. dbt model assets under `polymarket/wc2026/{staging,intermediate,marts,observability}/...`,
    `polymarket/us_midterms_2026/{staging,intermediate,marts,observability}/...`,
    and `international_results/wc2026/{staging,intermediate,marts,observability}/...`

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
- `polymarket_us_midterms_2026_full_pipeline`: US midterms market discovery, hourly odds refresh, and scoped dbt build (`tag:us_midterms_2026` only; no WC2026 or FIFA results assets).

For local CLI runs, prefer the Python module entrypoint if virtualenv console
scripts have stale shebangs:

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

## Schedules

Schedules are stopped by default.

- `polymarket_wc2026_hourly_odds_schedule`: every hour for `polymarket_wc2026_hourly_odds_ingest` (`fidelity=60`).
- `polymarket_us_midterms_2026_hourly_odds_schedule`: every hour for `polymarket_us_midterms_2026_hourly_odds_ingest` (`fidelity=60`).

Enable only after manual jobs are healthy:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

## Recovery

- Re-run `polymarket_wc2026_hourly_odds_ingest` for routine odds gaps.
- Re-run `international_results_wc2026_match_results_ingest` when the source CSV
  updates completed scores or fixtures.
- Run `polymarket_wc2026_dbt_build` after raw or ops table repairs.
- Prune old `polymarket_wc2026_raw.odds_history` rows with `make prune-odds-history` (default 365-day retention; use `--dry-run` on the script to preview).
- Reclaim DuckDB file dead space with `make compact-warehouse` after pruning or full refreshes.
- Use `scripts/profile_warehouse.py` to inspect relation counts and freshness without opening the database read-write.

## Landing And Finalization

Canonical raw and ops table schemas remain stable for operators and dbt. dlt
lands markets, odds-history batches, WC2026 registry batches, and pipeline
run-event batches; dlt stage tables and `_dlt*` metadata tables are internal.
Raw market-token mappings are extracted from the same Gamma payload as markets
and finalized through the canonical DuckDB helper.

Scheduler ledger rows, skip state, and daily odds aggregates remain custom SQL
finalizers because they preserve monotonic cursors, scheduler state, first-seen
skip timestamps, and aggregate rebuild semantics.
