# Operations

Use this page when running Dagster assets, jobs, schedules, or recovery paths.
For data outputs, see [Warehouse](warehouse.md) and
[Data Contracts](data-contracts.md).

The v0.1.x orchestration surface is WC2026-only Polymarket.

## Dagster Assets

The main asset key order is:

1. `polymarket/wc2026/raw/markets`
2. `polymarket/wc2026/raw/markets_snapshot`
3. `polymarket/wc2026/ops/market_scope_registry`
4. `polymarket/wc2026/raw/market_metadata_backfill`
5. `polymarket/wc2026/raw/token_odds_history_hourly`
6. dbt model assets under `polymarket/wc2026/{staging,intermediate,marts,observability}/...`

Flat Dagster op names remain source-first, for example
`polymarket_wc2026_raw_token_odds_history_hourly`.

## Jobs

- `polymarket_wc2026_market_registry_refresh`: WC2026 market discovery, registry refresh, and metadata backfill.
- `polymarket_wc2026_hourly_odds_ingest`: hourly WC2026 token odds refresh (trailing 30 days by default).
- `polymarket_wc2026_dbt_build`: dbt analytics build for the WC2026 mart surface, including knockout marts.
- `polymarket_wc2026_full_pipeline`: WC2026 market discovery, hourly odds refresh (trailing 30 days), and dbt analytics build.

## WC2026 Scope

The shipped Dagster jobs and dbt graph are fixed to `wc2026`.

- `polymarket/wc2026/raw/markets_snapshot` and `polymarket/wc2026/ops/market_scope_registry`
  refresh `polymarket_wc2026_ops.market_scope_registry` for WC2026.
- `polymarket/wc2026/raw/market_metadata_backfill` and
  `polymarket/wc2026/raw/token_odds_history_hourly` run over the fixed WC2026
  registry.
- dbt model assets under `polymarket/wc2026/...` build the fixed WC2026 dbt graph.

## Schedules

Schedules are stopped by default.

- `polymarket_wc2026_hourly_odds_schedule`: every hour for `polymarket_wc2026_hourly_odds_ingest` (`fidelity=60`).

Enable only after manual jobs are healthy:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

## Recovery

- Re-run `polymarket_wc2026_hourly_odds_ingest` for routine odds gaps.
- Run `polymarket_wc2026_dbt_build` after raw or ops table repairs.
- Prune old `polymarket_wc2026_raw.odds_history` rows with `make prune-odds-history` (default 365-day retention; use `--dry-run` on the script to preview).
- Reclaim DuckDB file dead space with `make compact-warehouse` after pruning or full refreshes.
- Use `scripts/profile_warehouse.py` to inspect relation counts and freshness without opening the database read-write.

## Landing And Finalization

Canonical raw and ops table schemas remain stable for operators and dbt. dlt now
lands markets, market-token batches, odds-history batches, WC2026 registry
batches, and pipeline run-event batches; dlt stage tables and `_dlt*` metadata
tables are internal.

Scheduler ledger rows, skip state, and daily odds aggregates remain custom SQL
finalizers because they preserve monotonic cursors, scheduler state, first-seen
skip timestamps, and aggregate rebuild semantics.
