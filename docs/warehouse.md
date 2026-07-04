# Warehouse

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo
root. OddsFox is designed for prediction-market data; the v0.1.x warehouse
schemas and relation names are Polymarket-specific because that is the first
shipped adapter. For public mart guarantees, see
[Data Contracts](data-contracts.md).

## Raw Tables

Schema: `wc2026_polymarket_raw`

- `markets`: dlt-owned Gamma market landing table with frozen column/type contract.
- `market_tokens`: one row per market with CLOB token JSON; current batches land through dlt staging and finalize into this canonical table with `INSERT OR REPLACE`.
- `odds_history`: point-in-time CLOB token prices. Indexed by the composite
  primary key `(clobTokenId, timestamp)` for idempotent upserts. Operators may
  prune rows older than 365 days with `make prune-odds-history` (manual; not automatic).
  Current batches land through dlt staging, then finalize with duplicate `(clobTokenId, timestamp)` last-write-wins semantics.
- `token_odds_daily`: daily token aggregates rebuilt by custom SQL finalizers from
  canonical `odds_history`.

## Ops Tables

Schema: `wc2026_polymarket_ops`

- `market_scope_registry`: market ids admitted to the WC2026 market scope; current batches
  land through dlt staging before the canonical upsert preserves existing non-null event fields.
- `token_sync_ledger`: per-token sync progress kept in custom SQL because cursor
  and scheduler-state merges are stateful.
- `token_sync_skips`: persisted skip reasons kept in custom SQL to preserve `created_at`.
- `pipeline_run_events`: append-only run metrics landed through dlt staging.
- `sync_run_metrics`: latest sync metrics and short history. If appending to
  `pipeline_run_events` fails, the latest payload includes
  `pipeline_run_event_append_failed` and `pipeline_run_event_append_error`.
- `scrape_metadata`: small key/value metadata used by backfill progress helpers.
- `market_metadata_unresolved`: retry ledger for unresolved metadata fields.

## dbt Schemas

- `wc2026_polymarket_staging`
- `wc2026_polymarket_intermediate`
- `wc2026_polymarket_marts`
- `wc2026_polymarket_observability`

## dbt Intermediate

Schema: `wc2026_polymarket_intermediate`

- `int_wc2026_polymarket_token_universe`: materialized canonical one-row-per-token
  join of market tokens to market labels, state, and volume.
- `int_wc2026_polymarket_markets`: markets admitted by the fixed WC2026 scope;
  one row per `(scope_name, market_id)`.
- `int_wc2026_polymarket_market_tokens`: WC2026 subset of the token universe.
- `int_wc2026_polymarket_token_daily_timeseries`: token-level daily odds joined to the token universe.

## dbt Marts

Schema: `wc2026_polymarket_marts`

- `wc2026_market_coverage`: market-level daily odds coverage rolled up from `wc2026_token_coverage`.
- `wc2026_token_coverage`: token-level health and coverage, including daily coverage,
  sync ledger state, persisted skip reason, gap diagnostics, and market fully
  checked rollups.
- `wc2026_token_hourly_odds`: full hourly OHLC odds time series for WC2026 tokens (dbt
  view over `odds_history`; not materialized to save disk).
- `wc2026_token_daily_odds`: full daily OHLC odds time series for WC2026 tokens (dbt
  view over `int_wc2026_polymarket_token_daily_timeseries`; not materialized to save disk).
- `wc2026_markets`: WC2026 market universe; one row per `(scope_name, market_id)`.

Schema: `wc2026_polymarket_observability`

- `wc2026_sync_run_observability`: run-level ingestion and odds-sync telemetry.

## dlt Landing And Canonical Tables

Canonical raw and ops table names and schemas remain stable. dlt owns batch
landing for `markets`, `market_tokens`, `odds_history`,
`market_scope_registry`, and `pipeline_run_events`; stage tables and `_dlt*`
metadata tables are internal implementation details.

`wc2026_polymarket_raw.markets` is created by `wc2026_polymarket_raw_markets`. The
`dbt-build-ci` target creates an empty source fixture only in its disposable
DuckDB database.

Manual reset:

```sql
DROP TABLE IF EXISTS wc2026_polymarket_raw.markets;
```

Then materialize `wc2026_polymarket_raw_markets`.
