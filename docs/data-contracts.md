# Data Contracts

This page summarizes the public analytics surface that downstream notebooks,
scripts, and operators should rely on. OddsFox is a prediction-market pipeline;
the current public marts are WC2026-only Polymarket outputs. Model-level column
docs and tests live in the dbt project.

## Public Marts

Schema: `polymarket_wc2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `polymarket_wc2026_markets` | One row per `(scope_name, market_id)` | Canonical WC2026 market universe with scope attribution. |
| `polymarket_wc2026_token_coverage` | One row per `clob_token_id` | Token health, daily coverage, sync ledger state, skip state, gap health, and market fully checked rollups. |
| `polymarket_wc2026_market_coverage` | One row per market | Market-level coverage rolled up from `polymarket_wc2026_token_coverage`. |
| `polymarket_wc2026_token_hourly_odds` | One row per `(clob_token_id, odds_hour_utc)` | Full hourly OHLC odds time series for WC2026 tokens (dbt table). |
| `polymarket_wc2026_token_daily_odds` | One row per `(clob_token_id, odds_date_utc)` | Full daily OHLC odds time series for WC2026 tokens (dbt table). |
| `polymarket_wc2026_knockout_markets` | One row per `clob_token_id` | WC2026 knockout stage market tokens with sports metadata, latest hourly price, and result metadata. |
| `polymarket_wc2026_knockout_token_hourly_odds` | One row per `(clob_token_id, odds_hour_epoch)` | Graph-ready WC2026 knockout hourly odds with stage/team classification and Yes/No sibling token IDs (dbt table). |

## Health And Observability

- Use `polymarket_wc2026_marts.polymarket_wc2026_token_coverage` for token health and market fully
  checked status.
- Use `polymarket_wc2026_marts.polymarket_wc2026_market_coverage` for market-level coverage summaries.
- Use `polymarket_wc2026_observability.polymarket_wc2026_sync_run_observability` for run-level ingestion
  telemetry, request counts, and sync metrics.

## Current Scope Rules

- `polymarket_wc2026_token_coverage` covers all staged tokens.
- `polymarket_wc2026_token_hourly_odds` and `polymarket_wc2026_token_daily_odds` are full time series
  for the fixed WC2026 registry.
- Use `polymarket_wc2026_market_registry_refresh`, `polymarket_wc2026_hourly_odds_ingest`,
  `polymarket_wc2026_dbt_build`, and `polymarket_wc2026_full_pipeline` for Dagster operations.
- `polymarket_wc2026_knockout_token_hourly_odds` is the WC2026-specific export surface for downstream knockout visualization artifacts.
- `polymarket_wc2026_token_hourly_odds` and `polymarket_wc2026_token_daily_odds` include `outcome_label`
  (e.g. Yes/No) resolved from `outcome_index`; no join to `polymarket_wc2026_markets` is
  required to interpret which side a row represents.
- After `make prune-odds-history`, `polymarket_wc2026_raw.odds_history` (and therefore
  `polymarket_wc2026_token_hourly_odds`) only guarantees the trailing ~365 days of source
  odds points unless you change the retention window.
- `int_polymarket_wc2026_markets` is the canonical market-level WC2026 scope (grain: `scope_name`, `market_id`).

## dbt Checks

`uv run make dbt-build` runs model builds plus generic and singular data tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- Token and market coverage consistency.
- WC2026 odds time series parity with the WC2026 token universe.
- WC2026 market scope.

## Breaking change: source-first namespace reset

Public mart, asset, job, script, and schema names now use the source-first
`polymarket_wc2026` namespace. Dagster asset keys are hierarchical under
`polymarket/wc2026/...`; jobs, op config keys, scripts, dbt relations, and
DuckDB schemas use flat `polymarket_wc2026_*` names.

There are no compatibility views, env aliases, or migration shims in v0.1.x.
Delete old local warehouse files (`rm oddsfox.duckdb*`) and rerun quickstart
after upgrading from older layouts.

Public time-series marts are materialized as dbt tables. If an existing local
DuckDB warehouse still has these relations as views, reset the warehouse or
drop the affected dbt schemas before rebuilding.

## Breaking change: token_latest_odds removed (v0.1.1)

There is no compatibility view or shim for `token_latest_odds`. Use the
time-series marts below instead.

To get the latest daily or hourly price for a token, query
`polymarket_wc2026_token_daily_odds` or `polymarket_wc2026_token_hourly_odds` with the maximum time
key per `clob_token_id`, or query the intermediate models directly.
