# Operations

Use this page when running Dagster assets, jobs, schedules, or recovery paths.
For data outputs, see [Warehouse](warehouse.md) and
[Data Contracts](data-contracts.md).

The project is broader than selected-scope, but the v0.1.x orchestration surface below
is the shipped Polymarket/selected-scope implementation.

## Dagster Assets

The main asset order is:

1. `dlt_polymarket_markets`
2. `polymarket_markets_snapshot`
3. `polymarket_market_scope_registry`
4. `polymarket_market_metadata_backfill`
5. `polymarket_token_odds_history`
6. `polymarket_token_odds_history_minutely`
7. `polymarket_dbt`

`polymarket_odds_repair` is an operator repair asset, not part of the routine full pipeline.

## Jobs

- `polymarket_ingest_full_refresh_events`: full selected-scope event/market discovery, registry refresh, metadata backfill, and odds sync.
- `polymarket_ingest_incremental`: metadata backfill and routine token odds sync.
- `polymarket_minutely_odds_ingest`: minutely odds refresh for high-volume markets in the selected market scopes (union).
- `dbt_full_refresh`: dbt analytics build.
- `polymarket_selected_scope_full_pipeline`: full ingest plus dbt build.

## Market Scopes

Operators select one or more Polymarket scope presets with `POLYMARKET_MARKET_SCOPES`
(CSV in `.env`; default `wc2026`). See [Configuration](configuration.md) for the
preset catalog and multi-scope examples.

- `polymarket_markets_snapshot` and `polymarket_market_scope_registry` loop once per
  selected scope and refresh `polymarket_ops.market_scope_registry` per scope.
- `polymarket_market_metadata_backfill`, `polymarket_token_odds_history`, and
  `polymarket_token_odds_history_minutely` run once over the union of selected scopes.
- `polymarket_dbt` passes `active_market_scopes` to dbt from the same env selection.

## Schedules

Schedules are stopped by default.

All minutely schedules target `polymarket_minutely_odds_ingest`:

- `polymarket_minutely_odds_schedule`: every 5 minutes.
- `polymarket_minutely_odds_cold_schedule`: hourly conservative refresh with cold run config.
- `polymarket_minutely_odds_live_schedule`: every minute when explicitly enabled.

Enable only after manual jobs are healthy:

```dotenv
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=true
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
```

Enable `POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED` only during intentional live operation.

If both `POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED` and
`POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED` are true, only the live schedule
runs; the five-minute and hourly schedules stay stopped and a warning is logged.

## Recovery

- Re-run `polymarket_ingest_incremental` for routine gaps.
- Run `polymarket_odds_repair` if the token sync ledger is inconsistent.
- Run `dbt_full_refresh` after raw or ops table repairs.
- Prune old `polymarket_raw.odds_history` rows with `make prune-odds-history` (default 365-day retention; use `--dry-run` on the script to preview).
- Reclaim DuckDB file dead space with `make compact-warehouse` after pruning or full refreshes.
- Use `scripts/profile_warehouse.py` to inspect relation counts and freshness without opening the database read-write.

## Landing And Finalization

Canonical raw and ops table schemas remain stable for operators and dbt. dlt now
lands markets, market-token batches, odds-history batches, selected-scope registry
batches, and pipeline run-event batches; dlt stage tables and `_dlt*` metadata
tables are internal.

Scheduler ledger rows, skip state, and daily odds aggregates remain custom SQL
finalizers because they preserve monotonic cursors, scheduler state, first-seen
skip timestamps, and aggregate rebuild semantics.
