# Data Contracts

This page summarizes the public analytics surface that downstream notebooks,
scripts, and operators should rely on. OddsFox is a prediction-market pipeline;
the current public marts are WC2026-only Polymarket outputs. Model-level column
docs and tests live in the dbt project.

## Public Marts

Schema: `wc2026_polymarket_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `wc2026_markets` | One row per `(scope_name, market_id)` | Canonical WC2026 market universe with scope attribution. |
| `wc2026_token_coverage` | One row per `clob_token_id` | Token health, daily coverage, sync ledger state, skip state, gap health, and market fully checked rollups. |
| `wc2026_market_coverage` | One row per market | Market-level coverage rolled up from `wc2026_token_coverage`. |
| `wc2026_token_minutely_odds` | One row per `(clob_token_id, odds_timestamp_epoch)` | Full minutely odds time series for WC2026 tokens (dbt view). |
| `wc2026_token_hourly_odds` | One row per `(clob_token_id, odds_hour_utc)` | Full hourly OHLC odds time series for WC2026 tokens (dbt view). |
| `wc2026_token_daily_odds` | One row per `(clob_token_id, odds_date_utc)` | Full daily OHLC odds time series for WC2026 tokens (dbt view). |
| `wc2026_whale_minutely_odds` | One row per token timestamp | Minutely odds for WC2026 markets above the configured volume threshold (filtered view over `wc2026_token_minutely_odds`). |
| `wc2026_knockout_markets` | One row per `clob_token_id` | WC2026 knockout stage market tokens with sports metadata, latest hourly price, and result metadata. |
| `wc2026_knockout_token_hourly_odds` | One row per `(clob_token_id, odds_hour_epoch)` | Graph-ready WC2026 knockout hourly odds with stage/team classification and Yes/No sibling token IDs. |

## Health And Observability

- Use `wc2026_polymarket_marts.wc2026_token_coverage` for token health and market fully
  checked status.
- Use `wc2026_polymarket_marts.wc2026_market_coverage` for market-level coverage summaries.
- Use `wc2026_polymarket_observability.wc2026_sync_run_observability` for run-level ingestion
  telemetry, request counts, and sync metrics.

## Current Scope Rules

- `wc2026_token_coverage` covers all staged tokens.
- `wc2026_token_minutely_odds`, `wc2026_token_hourly_odds`, and
  `wc2026_token_daily_odds` are full time series for the fixed WC2026 registry.
- Use `wc2026_market_registry_refresh`, `wc2026_minutely_odds_ingest`,
  `wc2026_hourly_odds_ingest`, `wc2026_dbt_build`, `wc2026_knockout_export`, and
  `wc2026_full_pipeline` for Dagster operations.
- `wc2026_knockout_token_hourly_odds` is the WC2026-specific export surface for downstream knockout visualization artifacts.
- `wc2026_token_minutely_odds`, `wc2026_token_hourly_odds`, `wc2026_token_daily_odds`, and `wc2026_whale_minutely_odds` include `outcome_label` (e.g. Yes/No) resolved from `outcome_index`; no join to `wc2026_markets` is required to interpret which side a row represents.
- `wc2026_whale_minutely_odds` is filtered by the fixed WC2026 registry and
  `wc2026_polymarket_whale_min_volume_usd`.
- After `make prune-odds-history`, `wc2026_polymarket_raw.odds_history` (and therefore
  `wc2026_token_minutely_odds`, `wc2026_token_hourly_odds`, and
  `wc2026_whale_minutely_odds`) only guarantees
  the trailing ~365 days of minutely points unless you change the retention window.
- `int_wc2026_polymarket_markets` is the canonical market-level WC2026 scope (grain: `scope_name`, `market_id`).

## dbt Checks

`uv run make dbt-build` runs model builds plus generic and singular data tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- Token and market coverage consistency.
- WC2026 odds time series parity with the WC2026 token universe.
- WC2026 market scope and whale volume threshold filtering.

## Breaking change: WC2026 namespace reset

Generic Polymarket and selected-named public mart, asset, job, script, and
schema names were removed. Use the `wc2026_*` Dagster jobs/assets,
`wc2026_polymarket_*` schemas, and the WC2026 marts listed above.

There are no compatibility views, env aliases, or migration shims in v0.1.x.
Delete old local warehouse files (`rm oddsfox.duckdb*`) and rerun quickstart
after upgrading from older layouts.

## Breaking change: token_latest_odds removed (v0.1.1)

There is no compatibility view or shim for `token_latest_odds`. Use the
time-series marts below instead.

To get the latest daily, hourly, or minutely price for a token, query
`wc2026_token_daily_odds`, `wc2026_token_hourly_odds`, or
`wc2026_token_minutely_odds` with the maximum time key per `clob_token_id`, or
query the intermediate models directly.
