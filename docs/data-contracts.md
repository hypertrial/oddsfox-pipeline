# Data Contracts

This page summarizes the public analytics surface that downstream notebooks,
scripts, and operators should rely on. OddsFox is a prediction-market pipeline;
the current public marts are WC2026-only Polymarket outputs. Model-level column
docs and tests live in the dbt project.

## Public Marts

Schema: `polymarket_wc2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `polymarket_wc2026_knockout_market_tokens` | One row per `clob_token_id` | Progression-side token universe for knockout-related markets with reported volume >= $5,000 USD. |
| `polymarket_wc2026_knockout_markets` | One row per `clob_token_id` | Latest progression-side knockout snapshot with market, team, stage, explicit market/price status, volume, and result metadata. |
| `polymarket_wc2026_knockout_token_hourly_odds` | One row per `(clob_token_id, odds_hour_epoch)` | Trailing 30-day hourly OHLC odds for progression-side knockout tokens, including live/historical status metadata. |

## Health And Observability

- Use `polymarket_wc2026_observability.polymarket_wc2026_sync_run_observability` for run-level ingestion
  telemetry, request counts, and sync metrics.
- Use `polymarket_wc2026_observability.polymarket_wc2026_knockout_stage_coverage` to inspect raw
  classified market coverage vs public scoped tokens by knockout stage, direction, and market status.
- Use `polymarket_wc2026_observability.polymarket_wc2026_knockout_data_quality` for source-state anomalies,
  sparse stage coverage, and stale or missing live odds findings.

## Current Scope Rules

- Public WC2026 marts expose only knockout-related markets from the WC2026 registry
  with reported `volume >= $5,000` USD. The floor is dynamic: markets crossing
  $5,000 on a later sync are admitted on the next dbt build.
- `stage_key` values are `winner`, `final`, `semifinal`, `quarterfinal`,
  `round_of_16`, and `round_of_32`.
- Public knockout odds are normalized to the progression side. Winner/reach markets
  use the Yes token; elimination-framed markets use the No token. Use
  `market_direction` and `source_outcome_label` to inspect the source framing.
- Public knockout marts keep historical closed/resolved rows. Filter `is_live_market = true`
  or `market_status = 'live'` when you need live odds only. `source_state_anomaly`
  marks upstream rows where Gamma reports both active and closed; the derived status
  treats those rows as closed.
- `polymarket_wc2026_knockout_markets.current_price_status` separates `fresh_live`,
  `stale_live`, `missing_live`, `historical_closed`, `historical_resolved`, and
  `inactive` rows. Live prices are fresh when the latest hourly close is no more
  than 3 hours old.
- `polymarket_wc2026_knockout_token_hourly_odds` aggregates directly from staged
  raw odds and the knockout token classifier, and only exposes the trailing 30
  days of hourly OHLC rows.
- Use `polymarket_wc2026_market_registry_refresh`, `polymarket_wc2026_hourly_odds_ingest`,
  `polymarket_wc2026_dbt_build`, and `polymarket_wc2026_full_pipeline` for Dagster operations.
- `polymarket_wc2026_knockout_token_hourly_odds` is the WC2026-specific export surface for downstream knockout visualization artifacts.
- After `make prune-odds-history`, `polymarket_wc2026_raw.odds_history` only guarantees the trailing ~365 days of source
  odds points unless you change the retention window.
- `int_polymarket_wc2026_markets` is the canonical market-level WC2026 scope (grain:
  `scope_name`, `market_id`) with the $5,000 volume floor applied.

## dbt Checks

`uv run make dbt-build` runs model builds plus generic and singular data tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- WC2026 market scope (`accepted_values` on `scope_name` and knockout `stage_key`).
- Knockout progression-side token selection, including elimination-framed No-token rows.
- Knockout volume floor and trailing 30-day hourly window.
- Knockout market and current-price status accepted values.
- Hard-fail DQ checks for error-severity rows in `polymarket_wc2026_knockout_data_quality`.
- Warn-level DQ checks for stale/missing live odds and unsurfaced source-state or hourly coverage issues.
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

The knockout hourly time-series mart is materialized as a dbt table. If an
existing local DuckDB warehouse still has deleted broad public marts or old
relation types, reset the warehouse or drop the affected dbt schemas before
rebuilding.
