# Naming

OddsFox names source-specific pipeline surfaces from source to scope to subject.
Current canonical tuples are:

- `source`: `polymarket`, `scope`: `wc2026`, `namespace`: `polymarket_wc2026`
- `source`: `polymarket`, `scope`: `us_midterms_2026`, `namespace`: `polymarket_us_midterms_2026`
- `source`: `international_results`, `scope`: `wc2026`, `namespace`: `international_results_wc2026`
- `source`: `kalshi`, `scope`: `wc2026`, `namespace`: `kalshi_wc2026`

Flat names use `<source>_<scope>_<subject>[_<cadence>]`. Use flat names for
Dagster jobs, schedules, op names, Python functions, env vars, scripts, dbt
relations, and DuckDB schemas.

Dagster asset keys are hierarchical so the asset graph remains readable as
more sources and scopes are added:

- `polymarket/wc2026/raw/markets`
- `polymarket/wc2026/ops/market_scope_registry`
- `polymarket/wc2026/marts/knockout_token_hourly_odds`
- `polymarket/us_midterms_2026/raw/markets`
- `polymarket/us_midterms_2026/ops/market_scope_registry`
- `polymarket/us_midterms_2026/marts/market_token_hourly_odds`
- `international_results/wc2026/raw/match_results`
- `international_results/wc2026/marts/team_status`
- `kalshi/wc2026/raw/markets`
- `kalshi/wc2026/ops/market_scope_registry`
- `kalshi/wc2026/raw/market_candlesticks_hourly`
- `kalshi/wc2026/marts/stage_markets`

DuckDB and dbt schemas use `<source>_<scope>_<layer>`:

- `polymarket_wc2026_raw`
- `polymarket_wc2026_ops`
- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`
- `polymarket_us_midterms_2026_raw`
- `polymarket_us_midterms_2026_ops`
- `polymarket_us_midterms_2026_staging`
- `polymarket_us_midterms_2026_intermediate`
- `polymarket_us_midterms_2026_marts`
- `polymarket_us_midterms_2026_observability`
- `international_results_wc2026_raw`
- `international_results_wc2026_staging`
- `international_results_wc2026_intermediate`
- `international_results_wc2026_marts`
- `international_results_wc2026_observability`
- `kalshi_wc2026_raw`
- `kalshi_wc2026_ops`
- `kalshi_wc2026_staging`
- `kalshi_wc2026_intermediate`
- `kalshi_wc2026_marts`
- `kalshi_wc2026_observability`

dbt model names use layer-specific prefixes:

- staging: `stg_polymarket_wc2026_<subject>`
- intermediate: `int_polymarket_wc2026_<subject>`
- marts and observability: `polymarket_wc2026_<subject>`
- staging: `stg_polymarket_us_midterms_2026_<subject>`
- intermediate: `int_polymarket_us_midterms_2026_<subject>`
- marts and observability: `polymarket_us_midterms_2026_<subject>`
- staging: `stg_international_results_wc2026_<subject>`
- intermediate: `int_international_results_wc2026_<subject>`
- marts and observability: `international_results_wc2026_<subject>`
- staging: `stg_kalshi_wc2026_<subject>`
- intermediate: `int_kalshi_wc2026_<subject>`
- marts and observability: `kalshi_wc2026_<subject>`

Dagster op names stay flat even when the asset key is hierarchical. For
example, the hourly odds asset key is
`polymarket/wc2026/raw/token_odds_history_hourly`, and its op config key is
`polymarket_wc2026_raw_token_odds_history_hourly`.
The US midterms hourly odds asset key is
`polymarket/us_midterms_2026/raw/token_odds_history_hourly`, and its op config key
is `polymarket_us_midterms_2026_raw_token_odds_history_hourly`.
The results asset key is `international_results/wc2026/raw/match_results`,
and its op name is `international_results_wc2026_raw_match_results`.
The Kalshi hourly candlesticks asset key is
`kalshi/wc2026/raw/market_candlesticks_hourly`, and its op config key is
`kalshi_wc2026_raw_market_candlesticks_hourly`.

This is a v0.1.x namespace reset. Operators with an older local warehouse
should stop Dagster, delete `oddsfox.duckdb*`, and rerun the quickstart.
