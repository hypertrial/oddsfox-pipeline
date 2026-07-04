# Naming

OddsFox names source-specific pipeline surfaces from source to scope to subject.
The current canonical tuple is:

- `source`: `polymarket`
- `scope`: `wc2026`
- `namespace`: `polymarket_wc2026`

Flat names use `<source>_<scope>_<subject>[_<cadence>]`. Use flat names for
Dagster jobs, schedules, op names, Python functions, env vars, scripts, dbt
relations, and DuckDB schemas.

Dagster asset keys are hierarchical so the asset graph remains readable as
more sources and scopes are added:

- `polymarket/wc2026/raw/markets`
- `polymarket/wc2026/ops/market_scope_registry`
- `polymarket/wc2026/marts/token_hourly_odds`

DuckDB and dbt schemas use `<source>_<scope>_<layer>`:

- `polymarket_wc2026_raw`
- `polymarket_wc2026_ops`
- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`

dbt model names use layer-specific prefixes:

- staging: `stg_polymarket_wc2026_<subject>`
- intermediate: `int_polymarket_wc2026_<subject>`
- marts and observability: `polymarket_wc2026_<subject>`

Dagster op names stay flat even when the asset key is hierarchical. For
example, the hourly odds asset key is
`polymarket/wc2026/raw/token_odds_history_hourly`, and its op config key is
`polymarket_wc2026_raw_token_odds_history_hourly`.

This is a v0.1.x namespace reset. Operators with an older local warehouse
should stop Dagster, delete `oddsfox.duckdb*`, and rerun the quickstart.
