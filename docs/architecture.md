# Architecture

OddsFox is intentionally local-first: every routine workflow writes to a local
DuckDB warehouse and is coordinated by Dagster jobs that can be inspected before
schedules are enabled. The project is a prediction-market pipeline; the current
v0.1.x adapter and marts focus on WC2026 Polymarket data.

At the generic layer, source adapters follow one shape: external market and
odds APIs feed dlt/Python ingestion, DuckDB stores raw and ops data, dbt
publishes local marts, and Dagster orchestrates the steps. Operators own the
resulting data in a local or self-managed warehouse; OddsFox does not host a
shared dataset.

## System Flow

Current WC2026 Polymarket implementation:

```mermaid
flowchart LR
    gamma["Prediction-market metadata API<br/>Polymarket Gamma in v0.1.x"] --> dlt["dlt market landing"]
    clob["Prediction-market odds API<br/>Polymarket CLOB in v0.1.x"] --> odds["Python odds sync"]
    dlt --> raw["DuckDB raw schema"]
    odds --> raw
    raw --> ops["DuckDB ops ledgers"]
    raw --> dbt["dbt models"]
    ops --> dbt
    dbt --> marts["Coverage and WC2026 odds marts"]
    dagster["Dagster jobs and schedules"] --> dlt
    dagster --> odds
    dagster --> dbt
```

Text fallback: prediction-market metadata and odds APIs feed DuckDB raw and ops
schemas. Dagster runs the ingest and dbt steps. dbt publishes local analytics
marts for coverage, health, and WC2026 odds time series.

The only supported market scope is `wc2026`; see [Configuration](configuration.md).

## Main Components

| Component | Responsibility |
| --- | --- |
| Dagster | Defines assets, jobs, and disabled-by-default schedules. |
| dlt | Lands market metadata and current raw/ops batches into DuckDB stage/canonical tables for the current adapter. |
| Python odds sync | Fetches odds, writes token history, and maintains ledgers. |
| DuckDB | Stores raw, ops, staging, intermediate, mart, and observability schemas. |
| dbt | Builds analytics models and data-contract tests. |

## Data Flow

```mermaid
flowchart TD
    raw["wc2026_polymarket_raw"] --> staging["wc2026_polymarket_staging"]
    ops["wc2026_polymarket_ops"] --> staging
    staging --> token_universe["int_wc2026_polymarket_token_universe"]
    staging --> wc2026_markets_int["int_wc2026_polymarket_markets"]
    ops --> wc2026_markets_int
    wc2026_markets_int --> wc2026_markets["wc2026_markets"]
    token_universe --> wc2026_tokens["int_wc2026_polymarket_market_tokens"]
    wc2026_markets_int --> wc2026_tokens
    token_universe --> coverage["wc2026_token_coverage"]
    coverage --> wc2026_market_coverage["wc2026_market_coverage"]
    wc2026_tokens --> hourly["wc2026_token_hourly_odds"]
    wc2026_tokens --> daily["wc2026_token_daily_odds"]
```

Text fallback: staging normalizes raw and ops tables, intermediates establish
token universes and WC2026 market scope rows, and marts publish token health,
market coverage, full WC2026 hourly/daily odds time series, and the WC2026
market universe (`scope_name`, `market_id`).

## Operating Model

- `wc2026_full_pipeline` is the one-click full manual pipeline.
- `wc2026_hourly_odds_ingest` is the hourly odds job (`fidelity=60`).
- Schedules are stopped by default and should stay off until manual runs pass.
- DuckDB allows one read-write writer, so scripts provide read-only inspection
  and repair paths for local operators.
