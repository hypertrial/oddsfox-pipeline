# Warehouse

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo
root. OddsFox is designed for prediction-market data; the v0.1.x warehouse
schemas and relation names are Polymarket-specific because that is the first
shipped adapter. For public mart guarantees, see
[Data Contracts](data-contracts.md).

## Raw Tables

Schema: `polymarket_wc2026_raw`

- `markets`: dlt-owned Gamma market landing table with frozen column/type contract.
- `market_tokens`: one row per market with CLOB token JSON; current batches land through dlt staging and finalize into this canonical table with `INSERT OR REPLACE`.
- `odds_history`: point-in-time CLOB token prices. Indexed by the composite
  primary key `(clobTokenId, timestamp)` for idempotent upserts. Operators may
  prune rows older than 365 days with `make prune-odds-history` (manual; not automatic).
  Current batches land through dlt staging, then finalize with duplicate `(clobTokenId, timestamp)` last-write-wins semantics.
- `token_odds_daily`: daily token aggregates rebuilt by custom SQL finalizers from
  canonical `odds_history`.

## Ops Tables

Schema: `polymarket_wc2026_ops`

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

- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`

## dbt Intermediate

Schema: `polymarket_wc2026_intermediate`

- `int_polymarket_wc2026_token_universe`: materialized canonical one-row-per-token
  join of market tokens to market labels, state, and volume.
- `int_polymarket_wc2026_markets`: markets admitted by the fixed WC2026 scope;
  one row per `(scope_name, market_id)` with the $5,000 knockout volume floor.
- `int_polymarket_wc2026_market_tokens`: WC2026 subset of the token universe.

## dbt Marts

Schema: `polymarket_wc2026_marts`

- `polymarket_wc2026_knockout_market_tokens`: progression-side token universe for knockout markets with reported volume >= $5,000 USD, plus derived `market_status` and live/historical flags.
- `polymarket_wc2026_knockout_token_hourly_odds`: trailing 30-day hourly OHLC odds for progression-side knockout tokens (dbt table), including propagated market status.
- `polymarket_wc2026_knockout_markets`: latest progression-side knockout snapshot with explicit current-price status. Use `is_live_market` or `current_price_status = 'fresh_live'` for live-only views; closed/resolved rows are retained as historical rows.

Schema: `polymarket_wc2026_observability`

- `polymarket_wc2026_sync_run_observability`: run-level ingestion and odds-sync telemetry.
- `polymarket_wc2026_knockout_stage_coverage`: raw classified market coverage vs public scoped tokens by stage, direction, and market status.
- `polymarket_wc2026_knockout_data_quality`: row-level DQ findings for source-state anomalies, sparse stage coverage, and stale or missing odds.

## dlt Landing And Canonical Tables

Canonical raw and ops table names and schemas remain stable. dlt owns batch
landing for `markets`, `market_tokens`, `odds_history`,
`market_scope_registry`, and `pipeline_run_events`; stage tables and `_dlt*`
metadata tables are internal implementation details.

`polymarket_wc2026_raw.markets` is created by `polymarket_wc2026_raw_markets`. The
`dbt-build-ci` target creates an empty source fixture only in its disposable
DuckDB database.

Manual reset:

```sql
DROP TABLE IF EXISTS polymarket_wc2026_raw.markets;
```

Then materialize `polymarket_wc2026_raw_markets`.

If an existing local DuckDB file has deleted broad public marts or old dbt
relation types, reset the local warehouse (`rm oddsfox.duckdb*`) or drop the
affected dbt schemas before rebuilding.
