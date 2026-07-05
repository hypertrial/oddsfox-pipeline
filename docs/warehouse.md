# Warehouse

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo
root. OddsFox is designed for prediction-market data; the v0.1.x warehouse
schemas and relation names are Polymarket-specific because that is the first
shipped adapter. For public mart guarantees, see
[Data Contracts](data-contracts.md).

## Raw Tables

Schema: `polymarket_wc2026_raw`

- `markets`: dlt-owned Gamma market landing table with frozen column/type contract.
- `market_tokens`: one row per market with CLOB token JSON; current batches are
  extracted from the same Gamma payload as `markets` and finalized into this
  canonical table with `INSERT OR REPLACE`.
- `odds_history`: point-in-time CLOB token prices. Indexed by the composite
  primary key `(clobTokenId, timestamp)` for idempotent upserts. Operators may
  prune rows older than 365 days with `make prune-odds-history` (manual; not automatic).
  Current batches land through dlt staging, then finalize with duplicate `(clobTokenId, timestamp)` last-write-wins semantics.
- `token_odds_daily`: daily token aggregates rebuilt by custom SQL finalizers from
  canonical `odds_history`.

Schema: `international_results_wc2026_raw`

- `match_results`: WC2026-only FIFA World Cup fixture/result rows from
  `martj42/international_results`. Ingestion refreshes this table as a full
  replacement and stores scheduled fixtures with null scores.

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
- `international_results_wc2026_staging`
- `international_results_wc2026_intermediate`
- `international_results_wc2026_marts`
- `international_results_wc2026_observability`

## dbt Intermediate

Schema: `polymarket_wc2026_intermediate`

- `int_polymarket_wc2026_token_universe`: materialized canonical one-row-per-token
  join of market tokens to market labels, state, and volume.
- `int_polymarket_wc2026_markets`: markets admitted by the fixed WC2026 scope;
  one row per `(scope_name, market_id)` with the knockout volume floor from the
  WC2026 contract seed.
- `int_polymarket_wc2026_market_tokens`: WC2026 subset of the token universe.
- `int_polymarket_wc2026_knockout_market_classification`: shared real-team
  knockout market classifier used by public knockout marts and observability.

## dbt Marts

Schema: `polymarket_wc2026_marts`

- `polymarket_wc2026_knockout_market_tokens`: progression-side token universe for real WC2026 team knockout markets with reported volume >= $5,000 USD, plus derived `market_status`, live/historical flags, and explicit price semantics.
- `polymarket_wc2026_knockout_token_hourly_odds`: trailing 30-day hourly OHLC odds for real-team progression-side knockout tokens (dbt table), including propagated market status, tournament status, and `price_represents = 'progression'`.
- `polymarket_wc2026_knockout_markets`: latest real-team progression-side knockout snapshot with explicit current-price status and progression outcome labels. Use `is_live_market` or `current_price_status = 'fresh_live'` for live-only views; closed/resolved rows are retained as historical rows.

Schema: `international_results_wc2026_marts`

- `international_results_wc2026_matches`: clean FIFA World Cup 2026 fixtures/results with stage mapping and tied-knockout advancer inference from later fixtures when possible.
- `international_results_wc2026_team_status`: canonical team roster and current tournament status used to filter Polymarket public marts.

Schema: `polymarket_wc2026_observability`

- `polymarket_wc2026_sync_run_observability`: run-level ingestion, market-discovery provenance, and odds-sync telemetry.
- `polymarket_wc2026_knockout_stage_coverage`: raw classified market coverage vs public scoped tokens by stage, direction, and market status.
- `polymarket_wc2026_knockout_data_quality`: DQ findings for aggregated source-state anomalies, sparse stage coverage, stale or missing odds, and live-team alignment.

Schema: `international_results_wc2026_observability`

- `international_results_wc2026_data_quality`: warning-level findings when a tied knockout match has no unique inferred advancer or when the fixture/result source load is stale under the WC2026 contract seed.

## dlt Landing And Canonical Tables

Canonical raw and ops table names and schemas remain stable. dlt owns batch
landing for `markets`, `market_tokens`, `odds_history`,
`market_scope_registry`, and `pipeline_run_events`; stage tables and `_dlt*`
metadata tables are internal implementation details.

`international_results_wc2026_raw.match_results` is custom SQL storage, not dlt,
because the source is a single CSV and a full WC2026 replacement is simpler than
batch finalization.

`polymarket_wc2026_raw.markets` is created by `polymarket_wc2026_raw_markets`.
That asset performs the single Gamma market discovery pass and persists token
mappings from the same payload after dlt market landing succeeds. The
`polymarket_wc2026_raw_markets_snapshot` asset is local lineage/accounting only.
The `dbt-build-ci` target creates an empty source fixture only in its disposable
DuckDB database.

Manual reset:

```sql
DROP TABLE IF EXISTS polymarket_wc2026_raw.markets;
```

Then materialize `polymarket_wc2026_raw_markets`.

If an existing local DuckDB file has deleted broad public marts or old dbt
relation types, reset the local warehouse (`rm oddsfox.duckdb*`) or drop the
affected dbt schemas before rebuilding.
