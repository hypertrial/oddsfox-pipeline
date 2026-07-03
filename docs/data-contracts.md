# Data Contracts

This page summarizes the public analytics surface that downstream notebooks,
scripts, and operators should rely on. OddsFox is a prediction-market pipeline;
the current public marts are Polymarket selected-scope outputs. Model-level column docs
and tests live in the dbt project.

## Public Marts

Schema: `polymarket_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `selected_markets` | One row per `(scope_name, market_id)` | Canonical selected market universe with scope attribution. |
| `token_coverage` | One row per `clob_token_id` | Token health, daily coverage, sync ledger state, skip state, gap health, and market fully checked rollups. |
| `market_coverage` | One row per market | Market-level coverage rolled up from `token_coverage`. |
| `selected_token_minutely_odds` | One row per `(clob_token_id, odds_timestamp_epoch)` | Full minutely odds time series for all selected tokens (dbt view). |
| `selected_token_hourly_odds` | One row per `(clob_token_id, odds_hour_utc)` | Full hourly OHLC odds time series for all selected tokens (dbt view). |
| `selected_token_live_hourly_odds` | One row per `(clob_token_id, odds_hour_utc)` | Same schema as `selected_token_hourly_odds`, but restricted to full history for markets whose latest complete hour is active, not closed, and within 48 hours of the mart's global max hour. |
| `selected_token_daily_odds` | One row per `(clob_token_id, odds_date_utc)` | Full daily OHLC odds time series for all selected tokens (dbt view). |
| `selected_whale_minutely_odds` | One row per token timestamp | Minutely odds for selected markets above the configured volume threshold (filtered view over `selected_token_minutely_odds`). |

## Health And Observability

- Use `polymarket_marts.token_coverage` for token health and market fully
  checked status.
- Use `polymarket_marts.market_coverage` for market-level coverage summaries.
- Use `polymarket_observability.sync_run_observability` for run-level ingestion
  telemetry, request counts, and sync metrics.

## Current Scope Rules

- `token_coverage` covers all staged tokens.
- `selected_token_minutely_odds`, `selected_token_hourly_odds`, and `selected_token_daily_odds` are full time series for the active selected market scopes (union across `POLYMARKET_MARKET_SCOPES`).
- `selected_token_live_hourly_odds` is the graph-ready live-current hourly export surface. It intentionally excludes closed and stale markets while preserving full hourly history for each admitted market.
- `selected_token_minutely_odds`, `selected_token_hourly_odds`, `selected_token_daily_odds`, and `selected_whale_minutely_odds` include `outcome_label` (e.g. Yes/No) resolved from `outcome_index`; no join to `selected_markets` is required to interpret which side a row represents.
- `selected_whale_minutely_odds` is filtered by the active selected market scopes and
  `polymarket_whale_min_volume_usd`.
- After `make prune-odds-history`, `polymarket_raw.odds_history` (and therefore
  `selected_token_minutely_odds`, `selected_token_hourly_odds`, and
  `selected_whale_minutely_odds`) only guarantees
  the trailing ~365 days of minutely points unless you change the retention window.
- `int_polymarket_selected_markets` is the canonical market-level selected scope (grain: `scope_name`, `market_id`).

## dbt Checks

`uv run make dbt-build` runs model builds plus generic and singular data tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- Token and market coverage consistency.
- selected-scope odds time series parity with selected token universes.
- selected market scope and whale volume threshold filtering.

## Breaking change: selected-scope contracts (v0.1.2)

WC2026-specific public mart, registry, asset, job, script, and env-var names
were replaced by selected-scope names. Use `POLYMARKET_MARKET_SCOPES`, dbt
`active_market_scopes`, `polymarket_selected_scope_full_pipeline`,
`polymarket_market_scope_registry`, and the `selected_*` marts. The
`selected_markets` grain is `(scope_name, market_id)`, and selected-scope odds
marts include `outcome_label`.

There are no compatibility views, env aliases, or migration shims in v0.1.x.
Delete old local warehouse files (`rm oddsfox.duckdb*`) and rerun quickstart
after upgrading from pre-`v0.1.2` layouts.

## Breaking change: token_latest_odds removed (v0.1.1)

There is no compatibility view or shim for `token_latest_odds`. Use the
time-series marts below instead.

To get the latest daily, hourly, or minutely price for a token, query
`selected_token_daily_odds`, `selected_token_hourly_odds`, or
`selected_token_minutely_odds` with the maximum time key per `clob_token_id`, or
query the intermediate models directly.
