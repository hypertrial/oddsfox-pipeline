# Orchestration reference

This reference lists the fixed Dagster assets, jobs, scope behavior, schedules,
and persistence boundaries shipped by OddsFox Pipeline `v0.1.x`.

For procedures, use [Run a scope](../guides/run-a-scope.md),
[Enable schedules](../guides/enable-schedules.md), and
[Validate and recover](../guides/validate-and-recover.md).

## Asset order

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
12. `kalshi/wc2026/raw/events` (landed with the markets dlt source)
13. `kalshi/wc2026/raw/markets`
14. `kalshi/wc2026/raw/markets_snapshot`
15. `kalshi/wc2026/ops/market_scope_registry`
16. `kalshi/wc2026/raw/market_candlesticks_hourly`
17. dbt model assets under the matching
    `{staging,intermediate,marts,observability}` namespaces.

Flat Dagster op names preserve the same source-first order, for example
`polymarket_wc2026_raw_token_odds_history_hourly`.

## Jobs

### Polymarket WC2026

- `polymarket_wc2026_market_registry_refresh`: market discovery, registry
  refresh, and metadata backfill.
- `polymarket_wc2026_hourly_odds_ingest`: trailing hourly token-odds refresh.
- `international_results_wc2026_match_results_ingest`: FIFA fixture/results
  refresh.
- `polymarket_wc2026_dbt_build`: WC2026 and international-results dbt build.
- `polymarket_wc2026_full_pipeline`: results, registry, odds, and dbt.

### Polymarket US midterms 2026

- `polymarket_us_midterms_2026_market_registry_refresh`
- `polymarket_us_midterms_2026_hourly_odds_ingest`
- `polymarket_us_midterms_2026_dbt_build`
- `polymarket_us_midterms_2026_full_pipeline`

The dbt jobs select `tag:us_midterms_2026`; there is no FIFA results input.

### Kalshi WC2026

- `kalshi_wc2026_market_registry_refresh`
- `kalshi_wc2026_hourly_odds_ingest`
- `kalshi_wc2026_dbt_build`
- `kalshi_wc2026_full_pipeline`

The full pipeline refreshes FIFA results, Kalshi markets and candlesticks, then
builds `+tag:kalshi` including `international_results` parents while excluding
unrelated Polymarket tests.

## Scope behavior

### Polymarket WC2026

- `raw/markets` performs one Gamma discovery pass, lands raw markets through
  dlt, and persists token mappings from the same payload.
- `raw/markets_snapshot` records local lineage and does not call Gamma.
- `ops/market_scope_registry` writes only when discovery did not already
  refresh the registry.
- Metadata backfill and hourly odds operate over the fixed WC2026 registry.
- FIFA results supply the real-team validation inputs used by dbt.

### Polymarket US midterms 2026

- Discovery targets Balance of Power, Senate control, and House control event
  slugs.
- Raw, ops, registry, and odds assets mirror the WC2026 flow in a separate
  namespace.
- The public dbt surface is a markets-plus-hourly-odds mart without office-type
  classification.

### Kalshi WC2026

- `raw/markets` discovers series, events, and markets and lands events and
  markets through dlt.
- `raw/markets_snapshot` is local lineage and does not call Kalshi.
- The registry admits fixed WC2026 stage and group-winner markets.
- `raw/market_candlesticks_hourly` syncs hourly public-trade-API candlesticks.

## Schedules

| Schedule | Job | Default |
| --- | --- | --- |
| `polymarket_wc2026_hourly_odds_schedule` | `polymarket_wc2026_hourly_odds_ingest` | Stopped |
| `polymarket_us_midterms_2026_hourly_odds_schedule` | `polymarket_us_midterms_2026_hourly_odds_ingest` | Stopped |
| `kalshi_wc2026_hourly_odds_schedule` | `kalshi_wc2026_hourly_odds_ingest` | Stopped |

All three use hourly fidelity (`fidelity=60`).

## Landing and finalization

Canonical raw and ops table schemas are the operator and dbt boundary. dlt
lands market, odds-history, registry, and pipeline-event batches; stage tables
and `_dlt*` metadata are internal.

International-results CSV storage and Kalshi candlesticks use custom SQL.
Scheduler ledger rows, skip state, and daily odds aggregates also remain custom
SQL finalizers because they preserve monotonic cursors, first-seen timestamps,
scheduler state, and aggregate rebuild semantics.

Next, see the [warehouse reference](warehouse.md) for relation ownership or
[data contracts](data-contracts.md) for the public analytics surface.
