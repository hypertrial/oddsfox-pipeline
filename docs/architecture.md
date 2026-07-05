# Architecture

OddsFox is intentionally local-first: every routine workflow writes to a local
DuckDB warehouse and is coordinated by Dagster jobs that can be inspected before
schedules are enabled. The project is a prediction-market pipeline; the current
v0.1.x adapter and marts focus on WC2026 Polymarket data, with FIFA World Cup
fixture/results rows used to validate real team scope.

At the generic layer, source adapters follow one shape: external market and
odds APIs feed dlt/Python ingestion, DuckDB stores raw and ops data, dbt
publishes local marts, and Dagster orchestrates the steps. Operators own the
resulting data in a local or self-managed warehouse; OddsFox does not host a
shared dataset.

## System Flow

Current WC2026 implementation:

```mermaid
flowchart LR
    gamma["Prediction-market metadata API<br/>Polymarket Gamma in v0.1.x"] --> dlt["dlt market landing"]
    clob["Prediction-market odds API<br/>Polymarket CLOB in v0.1.x"] --> odds["Python odds sync"]
    results["FIFA results CSV<br/>international_results"] --> result_sync["Python CSV sync"]
    dlt --> raw["DuckDB raw schema"]
    odds --> raw
    result_sync --> raw
    raw --> ops["DuckDB ops ledgers"]
    raw --> dbt["dbt models"]
    ops --> dbt
    dbt --> marts["WC2026 knockout odds marts"]
    dagster["Dagster jobs and schedules"] --> dlt
    dagster --> odds
    dagster --> result_sync
    dagster --> dbt
```

Text fallback: prediction-market metadata/odds APIs and the FIFA results CSV
feed DuckDB raw and ops schemas. Dagster runs the ingest and dbt steps. dbt
publishes local analytics marts for WC2026 knockout odds, team scope, and
ingestion observability.

The shipped Dagster/dbt graph is fixed to `wc2026`; see
[Configuration](configuration.md) for the seed-backed helper boundary.

## Main Components

| Component | Responsibility |
| --- | --- |
| Dagster | Defines assets, jobs, and disabled-by-default schedules. |
| dlt | Lands market metadata and current raw/ops batches into DuckDB stage/canonical tables for the current adapter. |
| Python CSV sync | Loads the WC2026 FIFA World Cup fixture/result slice used for team validation. |
| Python odds sync | Fetches odds, writes token history, and maintains ledgers. |
| DuckDB | Stores raw, ops, staging, intermediate, mart, and observability schemas. |
| dbt | Builds analytics models and data-contract tests. |

## Data Flow

```mermaid
flowchart TD
    raw["polymarket_wc2026_raw"] --> staging["polymarket_wc2026_staging"]
    results_raw["international_results_wc2026_raw"] --> results_staging["international_results_wc2026_staging"]
    results_staging --> matches["international_results_wc2026_matches"]
    matches --> team_status["international_results_wc2026_team_status"]
    ops["polymarket_wc2026_ops"] --> staging
    staging --> token_universe["int_polymarket_wc2026_token_universe"]
    staging --> wc2026_markets_int["int_polymarket_wc2026_markets"]
    staging --> odds["stg_polymarket_wc2026_odds"]
    ops --> wc2026_markets_int
    token_universe --> wc2026_tokens["int_polymarket_wc2026_market_tokens"]
    wc2026_markets_int --> wc2026_tokens
    team_status --> knockout_tokens
    wc2026_tokens --> knockout_tokens["polymarket_wc2026_knockout_market_tokens"]
    odds --> hourly_fact["int_polymarket_wc2026_token_hourly_odds"]
    hourly_fact --> knockout_hourly
    knockout_tokens --> knockout_hourly["polymarket_wc2026_knockout_token_hourly_odds"]
    knockout_hourly --> knockout_markets["polymarket_wc2026_knockout_markets"]
    staging --> stage_coverage["polymarket_wc2026_knockout_stage_coverage"]
    knockout_tokens --> stage_coverage
    knockout_hourly --> stage_coverage
    knockout_markets --> data_quality["polymarket_wc2026_knockout_data_quality"]
    stage_coverage --> data_quality
    ops --> observability["polymarket_wc2026_sync_run_observability"]
    matches --> results_dq["international_results_wc2026_data_quality"]
```

Text fallback: staging normalizes raw and ops tables, intermediates establish
token universes and WC2026 market scope rows, FIFA result marts provide real
team status, and Polymarket marts publish cleaned knockout progression-side
token odds plus latest knockout snapshots. Observability models publish run
metrics, stage coverage, result inference warnings, and DQ findings for
live/historical status, active-team live consumption, odds freshness, and sparse
team coverage.

## Operating Model

- `polymarket_wc2026_full_pipeline` is the one-click full manual pipeline.
- `international_results_wc2026_match_results_ingest` refreshes fixture/results
  and also runs inside the full pipeline.
- `polymarket_wc2026_hourly_odds_ingest` is the hourly odds job (`fidelity=60`).
- Schedules are stopped by default and should stay off until manual runs pass.
- DuckDB allows one read-write writer, so scripts provide read-only inspection
  and repair paths for local operators.
