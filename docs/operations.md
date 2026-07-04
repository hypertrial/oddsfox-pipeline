# Operations

Use this page when running Dagster assets, jobs, schedules, or recovery paths.
For data outputs, see [Warehouse](warehouse.md) and
[Data Contracts](data-contracts.md).

The v0.1.x orchestration surface is WC2026-only Polymarket.

## Dagster Assets

The main asset order is:

1. `wc2026_polymarket_raw_markets`
2. `wc2026_polymarket_markets_snapshot`
3. `wc2026_polymarket_market_registry`
4. `wc2026_polymarket_market_metadata_backfill`
5. `wc2026_polymarket_token_odds_history_hourly`
6. `wc2026_polymarket_dbt`

## Jobs

- `wc2026_market_registry_refresh`: WC2026 market discovery, registry refresh, and metadata backfill.
- `wc2026_hourly_odds_ingest`: hourly WC2026 token odds refresh.
- `wc2026_dbt_build`: dbt analytics build for the WC2026 mart surface, including knockout marts.
- `wc2026_full_pipeline`: WC2026 market discovery, hourly odds refresh, and dbt analytics build.

## WC2026 Scope

`wc2026` is the only supported market scope.

- `wc2026_polymarket_markets_snapshot` and `wc2026_polymarket_market_registry`
  refresh `wc2026_polymarket_ops.market_scope_registry` for WC2026.
- `wc2026_polymarket_market_metadata_backfill`,
  `wc2026_polymarket_token_odds_history_hourly` run over the fixed WC2026
  registry.
- `wc2026_polymarket_dbt` builds the fixed WC2026 dbt graph.

## Schedules

Schedules are stopped by default.

- `wc2026_hourly_odds_schedule`: every hour for `wc2026_hourly_odds_ingest` (`fidelity=60`).

Enable only after manual jobs are healthy:

```dotenv
WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

## Recovery

- Re-run `wc2026_hourly_odds_ingest` for routine odds gaps.
- Run `wc2026_dbt_build` after raw or ops table repairs.
- Prune old `wc2026_polymarket_raw.odds_history` rows with `make prune-odds-history` (default 365-day retention; use `--dry-run` on the script to preview).
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
