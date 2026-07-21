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
6. `polymarket/wc2026/raw/match_token_odds_history_minute` (dedicated backfill only)
7. `polymarket/us_midterms_2026/raw/markets`
8. `polymarket/us_midterms_2026/raw/markets_snapshot`
9. `polymarket/us_midterms_2026/ops/market_scope_registry`
10. `polymarket/us_midterms_2026/raw/market_metadata_backfill`
11. `polymarket/us_midterms_2026/raw/token_odds_history_hourly`
12. `international_results/historical/raw/snapshot`
13. `international_results/wc2026/raw/match_results`
14. `openfootball/wc2026/raw/knockout_fixtures`
15. `kalshi/wc2026/raw/events` (landed with the markets dlt source)
16. `kalshi/wc2026/raw/markets`
17. `kalshi/wc2026/raw/markets_snapshot`
18. `kalshi/wc2026/ops/market_scope_registry`
19. `kalshi/wc2026/raw/market_candlesticks_hourly`
20. dbt model assets under the matching
    `{staging,intermediate,marts,observability}` namespaces.

Flat Dagster op names preserve the same source-first order, for example
`polymarket_wc2026_raw_token_odds_history_hourly`.

## Jobs

### Polymarket WC2026

- `international_results_historical_ingest`: public 2006+ matches, shootouts,
  and goalscorers for strategy model fitting.
- `polymarket_wc2026_market_registry_refresh`: market discovery, registry
  refresh, and metadata backfill.
- `polymarket_wc2026_hourly_odds_ingest`: trailing hourly token-odds refresh.
- `polymarket_wc2026_match_minute_odds_backfill`: one-time or rerunnable
  completed-match backfill for all 104 FIFA-numbered games and the dedicated
  minute mart. It refreshes the latest 104 international-results rows and all 32
  OpenFootball knockout fixtures, discovers closed Gamma events without a volume
  floor, validates result alignment and the 104/248/496 inventory, fetches exact
  game windows at CLOB `fidelity=1`, then runs dbt. The results refresh first
  resolves and downloads an immutable Git revision. Minute fetches append 496
  audit rows; only an all-success run atomically replaces raw history and marks
  those audits published.
  Run `uv run make match-minute-live-smoke` for the disposable live acceptance
  check; it is intentionally absent from CI and all schedules.
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

### Cross-platform WC2026 knockout match odds

- `wc2026_knockout_match_odds_full_pipeline`: refreshes the OpenFootball
  fixture mirror, both provider registries, both hourly odds sources, permanent
  provider facts, and the neutral mart/observability models in one job.

The combined job selects `+tag:cross_domain`. Source-specific Polymarket and
Kalshi dbt jobs exclude that tag, so they cannot publish a partially refreshed
cross-provider comparison.

## Scope behavior

### Polymarket WC2026

- `raw/markets` performs one Gamma discovery pass, lands raw markets through
  dlt, and persists token mappings from the same payload.
- `raw/markets_snapshot` records local lineage and does not call Gamma.
- `ops/market_scope_registry` writes only when discovery did not already
  refresh the registry.
- Metadata backfill and hourly odds operate over the fixed WC2026 registry.
- The match-minute asset writes a separate raw table and never reads or updates
  the hourly token-sync ledger. Any missing token history aborts before dbt. A
  failed run keeps its append-only audit evidence while leaving the previous raw
  snapshot and public table intact.
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
- The registry admits fixed WC2026 stage, group-winner, and `KXWCADVANCE`
  match-advance markets.
- `raw/market_candlesticks_hourly` syncs hourly public-trade-API candlesticks.

### Canonical knockout fixtures

- `openfootball/wc2026/raw/knockout_fixtures` refreshes the dependency-free
  OpenFootball mirror of the FIFA schedule and retains FIFA match numbers
  73–104, including the third-place source row.
- The parser fails closed on changed IDs or stage assignments. Neutral dbt
  models exclude match 103 and map both vendors by unique normalized team pair.

## Schedules

| Schedule | Job | Default |
| --- | --- | --- |
| `international_results_daily_schedule` | `international_results_historical_ingest` | Stopped |
| `polymarket_wc2026_hourly_odds_schedule` | `polymarket_wc2026_hourly_odds_ingest` | Stopped |
| `polymarket_us_midterms_2026_hourly_odds_schedule` | `polymarket_us_midterms_2026_hourly_odds_ingest` | Stopped |
| `kalshi_wc2026_hourly_odds_schedule` | `kalshi_wc2026_hourly_odds_ingest` | Stopped |
| `wc2026_knockout_match_odds_hourly_schedule` | `wc2026_knockout_match_odds_full_pipeline` | Stopped |

The match-minute backfill has no schedule or environment enable flag.

The international-results schedule runs daily at 02:15 UTC; the other four run
hourly. The combined schedule uses Polymarket CLOB
`fidelity=60`, bypasses the progression-futures volume floor for exact match
markets, and remains stopped unless its dedicated env flag is enabled.

## Landing and finalization

Canonical raw and ops table schemas are the operator and dbt boundary. dlt
lands market, odds-history, registry, and pipeline-event batches; stage tables
and `_dlt*` metadata are internal.

International-results CSV storage, canonical snapshot loading, OpenFootball
fixture storage, and Kalshi candlesticks use custom transactional SQL.
Scheduler ledger rows, skip state, and daily odds aggregates also remain custom
SQL finalizers because they preserve monotonic cursors, first-seen timestamps,
scheduler state, and aggregate rebuild semantics.

Next, see the [warehouse reference](warehouse.md) for relation ownership or
[data contracts](data-contracts.md) for the public analytics surface.
