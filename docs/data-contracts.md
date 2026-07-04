# Data Contracts

This page summarizes the public analytics surface that downstream notebooks,
scripts, and operators should rely on. OddsFox is a prediction-market pipeline;
the current public marts are WC2026-only Polymarket outputs. Model-level column
docs and tests live in the dbt project.

## Public Marts

Schema: `polymarket_wc2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `polymarket_wc2026_markets` | One row per `(scope_name, market_id)` | Canonical WC2026 market universe: registry scope ∩ volume >= $100k USD. |
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

- Public WC2026 marts (`polymarket_wc2026_markets`, `polymarket_wc2026_market_tokens`, odds
  time series, and coverage) use the volume-scoped universe: `market_scope_registry`
  (scope=wc2026) ∩ reported `volume >= $100,000` USD. The floor is dynamic — markets
  crossing $100k on a later sync are admitted on the next dbt build.
- Knockout marts inherit this universe from `polymarket_wc2026_market_tokens` and apply
  only knockout-stage classification downstream.
- `polymarket_wc2026_token_coverage` covers every outcome token in the volume-scoped universe.
- `polymarket_wc2026_token_hourly_odds` and `polymarket_wc2026_token_daily_odds` are full time series
  for the volume-scoped WC2026 universe.
- Use `polymarket_wc2026_market_registry_refresh`, `polymarket_wc2026_hourly_odds_ingest`,
  `polymarket_wc2026_dbt_build`, and `polymarket_wc2026_full_pipeline` for Dagster operations.
- `polymarket_wc2026_knockout_token_hourly_odds` is the WC2026-specific export surface for downstream knockout visualization artifacts.
- `polymarket_wc2026_token_hourly_odds` and `polymarket_wc2026_token_daily_odds` include `outcome_label`
  (e.g. Yes/No) resolved from `outcome_index`; no join to `polymarket_wc2026_markets` is
  required to interpret which side a row represents.
- After `make prune-odds-history`, `polymarket_wc2026_raw.odds_history` (and therefore
  `polymarket_wc2026_token_hourly_odds`) only guarantees the trailing ~365 days of source
  odds points unless you change the retention window.
- `int_polymarket_wc2026_markets` is the canonical market-level WC2026 scope (grain:
  `scope_name`, `market_id`) with the $100k volume floor applied.

## dbt Checks

`uv run make dbt-build` runs model builds plus generic and singular data tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- Token and market coverage consistency.
- WC2026 odds time series parity with the WC2026 token universe (`assert_polymarket_wc2026_token_hourly_odds_contract`, `assert_polymarket_wc2026_token_daily_odds_contract`).
- WC2026 market scope (`accepted_values` on `scope_name` and knockout `stage_key`).
- Knockout Yes/No sibling token consistency.
- Observability run health (warn-level: latest run error-token regression and history coverage floor).

Warn-level observability tests fail softly in `dbt build` output; treat warnings as operator signals on real warehouses, not hard CI blockers when the disposable CI fixture is healthy.

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
