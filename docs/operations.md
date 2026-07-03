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
5. `wc2026_polymarket_token_odds_history_minutely`
6. `wc2026_polymarket_token_odds_history_hourly` (optional hourly-grain refresh)
7. `wc2026_polymarket_dbt`

`wc2026_polymarket_odds_repair` is an operator repair asset, not part of the routine full pipeline.

## Jobs

- `wc2026_market_registry_refresh`: WC2026 market discovery, registry refresh, and metadata backfill.
- `wc2026_minutely_odds_ingest`: minutely WC2026 token odds refresh.
- `wc2026_hourly_odds_ingest`: hourly WC2026 token odds refresh.
- `wc2026_dbt_build`: dbt analytics build for the WC2026 mart surface.
- `wc2026_knockout_export`: build the graph-facing WC2026 knockout hourly mart.
- `wc2026_full_pipeline`: WC2026 market discovery, minutely/hourly odds refresh, and dbt analytics build.

## WC2026 Scope

`wc2026` is the only supported market scope.

- `wc2026_polymarket_markets_snapshot` and `wc2026_polymarket_market_registry`
  refresh `wc2026_polymarket_ops.market_scope_registry` for WC2026.
- `wc2026_polymarket_market_metadata_backfill`,
  `wc2026_polymarket_token_odds_history_minutely`, and
  `wc2026_polymarket_token_odds_history_hourly` run over the fixed WC2026
  registry.
- `wc2026_polymarket_dbt` builds the fixed WC2026 dbt graph.

## Schedules

Schedules are stopped by default.

All minutely schedules target `wc2026_minutely_odds_ingest`:

- `wc2026_minutely_odds_schedule`: every 5 minutes.
- `wc2026_minutely_odds_cold_schedule`: hourly conservative trigger for the minutely job with cold run config.
- `wc2026_minutely_odds_live_schedule`: every minute when explicitly enabled.

The hourly data schedule is separate:

- `wc2026_hourly_odds_schedule`: every hour for `wc2026_hourly_odds_ingest` (`fidelity=60`).

Enable only after manual jobs are healthy:

```dotenv
WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=true
WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

Enable `WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED` only during intentional live operation.

If both `WC2026_POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED` and
`WC2026_POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED` are true, only the live schedule
runs; the five-minute and cold minutely schedules stay stopped and a warning is logged.

## Recovery

- Re-run `wc2026_minutely_odds_ingest` or `wc2026_hourly_odds_ingest` for routine odds gaps.
- Run `wc2026_polymarket_odds_repair` if the token sync ledger is inconsistent.
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
