# Data Contracts

This page summarizes the public analytics surface that downstream notebooks,
scripts, and operators should rely on. OddsFox Pipeline is a prediction-market pipeline;
the current public marts are WC2026 Polymarket knockout odds outputs, Kalshi WC2026
stage and group-winner odds, US midterms 2026 generic market odds, plus WC2026
FIFA World Cup fixtures/results used to validate WC2026 team scope. Model-level
column docs and tests live in the dbt project.

For practical analyst workflows, use
[Query the warehouse](../guides/query-the-warehouse.md),
[Query recipes](../guides/query-recipes.md), and
[Data dictionary](data-dictionary.md).
This page remains the formal contract summary.

## Public Marts

Schema: `wc2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `wc2026_knockout_match_hourly_odds` | One row per `(fifa_match_id, odds_hour_epoch)` | Dense hourly raw closing prices for both teams to advance from each FIFA-numbered knockout match on Polymarket and Kalshi. Covers match numbers 73–102 and 104; match 103 is excluded. |

`fifa_match_id` is the published numeric match number from the FIFA schedule,
not the repository-generated hash in the international-results mart. The
OpenFootball WC2026 file is the machine-readable mirror used for automation;
FIFA remains the identity authority. Fixture matching uses the normalized,
unordered pair of teams, then applies the fixture's official home/away order.
Provider ordering is never authoritative.

The four price columns are
`polymarket_home_advance_price`, `polymarket_away_advance_price`,
`kalshi_home_advance_price`, and `kalshi_away_advance_price`. They are raw
hourly closes in `[0, 1]` for the team that advances, including extra time and
penalties. For match 104, advancing means winning the World Cup. Prices are not
vig-adjusted, averaged across providers, interpolated, forward-filled, or
renormalized. `price_represents = 'team_advances'` and
`price_statistic = 'hourly_close'` make those semantics explicit.

Each match gets an hourly spine from its first through last observation on
either provider. A null price means that exact provider/side had no observation
in that exact hour. Use `polymarket_hour_complete`, `kalshi_hour_complete`, and
`both_sources_complete` to select complete comparisons. Pair sums are raw
diagnostics, not normalization factors. `is_pre_kickoff` distinguishes pregame
hours from in-play and settlement hours.

The platform facts retain old match hours across incremental runs and reprocess
a short recent lookback for late arrivals. No automatic age deletion applies to
these match facts. An intentional local warehouse reset still removes history.
The progression-futures and stage-of-elimination marts below have different
semantics and do not feed this relation.

Core and provenance fields:

| Field group | Columns |
| --- | --- |
| Time and identity | `odds_hour_utc`, `odds_hour_epoch`, `fifa_match_id`, `stage_key`, `stage_rank`, `kickoff_at_utc`, `home_team`, `away_team` |
| Polymarket provenance | `polymarket_market_id`, `polymarket_home_clob_token_id`, `polymarket_away_clob_token_id`, home/away observation counts |
| Kalshi provenance | `kalshi_event_ticker`, `kalshi_home_market_ticker`, `kalshi_away_market_ticker`, home/away hourly volumes |
| Diagnostics | pair-price sums, per-provider completeness, `both_sources_complete`, `is_pre_kickoff` |

Example: compare complete pregame hours without changing raw prices.

```sql
select
    odds_hour_utc,
    fifa_match_id,
    stage_key,
    home_team,
    away_team,
    polymarket_home_advance_price,
    kalshi_home_advance_price,
    polymarket_away_advance_price,
    kalshi_away_advance_price
from wc2026_marts.wc2026_knockout_match_hourly_odds
where both_sources_complete and is_pre_kickoff
order by fifa_match_id, odds_hour_epoch;
```

Schema: `polymarket_us_midterms_2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `polymarket_us_midterms_2026_market_token_hourly_odds` | One row per `(clob_token_id, odds_hour_epoch)` | Trailing 30-day hourly OHLC odds for admitted US midterms 2026 market tokens joined to source market metadata. No office-type classification in v0.1.x. |

Schema: `polymarket_wc2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `polymarket_wc2026_knockout_market_tokens` | One row per `clob_token_id` | Progression-side token universe for knockout-related markets at or above the WC2026 contract volume floor, including explicit price semantics. |
| `polymarket_wc2026_knockout_markets` | One row per `clob_token_id` | Latest progression-side knockout snapshot with market, team, stage, explicit market/price status, volume, result metadata, and price semantics. |
| `polymarket_wc2026_knockout_token_hourly_odds` | One row per `(clob_token_id, odds_hour_epoch)` | Trailing 30-day hourly OHLC odds for progression-side knockout tokens, including live/historical status metadata and price semantics. |
| `polymarket_wc2026_graph_token_hourly_odds` | One row per `(market_id, clob_token_id, odds_hour_epoch)` | Graph-build export with both Yes and No tokens per real-team knockout market plus dbt-clean stage/team/progression semantics. |

Schema: `international_results_wc2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `international_results_wc2026_matches` | One row per `match_id` | Clean WC2026 FIFA World Cup fixture/result rows from `martj42/international_results`, including stage, status, score, and inferred knockout advancer metadata. |
| `international_results_wc2026_team_status` | One row per `team_name` | Canonical 48-team WC2026 roster and current tournament status derived from fixture/result rows. |

Schema: `kalshi_wc2026_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `kalshi_wc2026_stage_markets` | One row per `market_ticker` | Latest stage-of-elimination market snapshot with team/stage classification, progression-side pricing, and current-price status. |
| `kalshi_wc2026_stage_market_hourly_odds` | One row per `(market_ticker, odds_hour_epoch)` | Trailing contract-window hourly OHLC odds for stage markets joined to classified metadata. |
| `kalshi_wc2026_group_winner_markets` | One row per `market_ticker` | Latest group-winner market snapshot with team classification and current-price status. |
| `kalshi_wc2026_group_winner_market_hourly_odds` | One row per `(market_ticker, odds_hour_epoch)` | Trailing contract-window hourly OHLC odds for group-winner markets. |

## Health And Observability

- Use `polymarket_us_midterms_2026_observability.polymarket_us_midterms_2026_sync_run_observability`
  for US midterms run-level ingestion telemetry.
- Use `polymarket_wc2026_observability.polymarket_wc2026_sync_run_observability` for run-level ingestion
  telemetry, market-discovery provenance, request counts, and sync metrics.
- Use `polymarket_wc2026_observability.polymarket_wc2026_knockout_stage_coverage` to inspect raw
  classified market coverage vs public scoped tokens by knockout stage, direction, and market status,
  including hourly completeness against the contract seed window.
- Use `polymarket_wc2026_observability.polymarket_wc2026_knockout_data_quality` for source-state anomalies,
  sparse stage/team coverage, upstream eliminated-team live lag, and actionable stale or missing live odds findings.
- Use `kalshi_wc2026_observability.kalshi_wc2026_sync_run_observability` for Kalshi run-level ingestion telemetry.
- Use `kalshi_wc2026_observability.kalshi_wc2026_stage_coverage` to inspect classified market coverage and hourly completeness against the contract seed window.
- Use `kalshi_wc2026_observability.kalshi_wc2026_data_quality` for Kalshi source-state anomalies, sparse coverage, and stale or missing live odds findings.
- Use `wc2026_observability.wc2026_knockout_match_odds_coverage` for one row per
  expected advancement match, including fixture readiness, both vendor mappings,
  side completeness, first/last observed hours, and freshness warnings.
- Use `wc2026_observability.wc2026_knockout_match_odds_data_quality` for hard
  mapping, fixture, and price errors plus missing/stale vendor warnings.

## Current Scope Rules

- Public US midterms 2026 marts expose only targeted Balance of Power, Senate
  control, and House control markets from the `us_midterms_2026` registry at or
  above the contract volume floor ($5,000 USD by default).
- **Balance of Power semantics:** each combo is an independent binary Yes/No
  market. Probabilities across combos do **not** sum to 1.0 (unlike mutually
  exclusive partitions).
- **Volume floor exclusions:** zero-volume placeholder markets (for example
  generic "Party A/B/C" rows) are intentionally excluded from public marts.
- **No office-type classification** in v0.1.x; join on `market_id` / `clob_token_id`
  and source question text.
- Shared US midterms thresholds live in
  `dbt/seeds/polymarket_us_midterms_2026_contract.csv`; there is no results or
  candidate validation layer for this scope in v0.1.x.
- Public Kalshi WC2026 marts expose stage-of-elimination and group-winner markets
  from the fixed `wc2026` registry across the packaged Kalshi series tickers.
  Shared Kalshi thresholds live in `dbt/seeds/kalshi_wc2026_contract.csv`.
- The neutral match mart admits exact `soccer_team_to_advance` Polymarket
  markets and exact `KXWCADVANCE` Kalshi markets regardless of volume. The
  source-specific $5,000 progression-futures filter remains unchanged.
- Public WC2026 marts expose only knockout-related markets from the WC2026 registry
  at or above the WC2026 contract volume floor. The current floor is $5,000 USD,
  and markets crossing it on a later sync are admitted on the next dbt build.
- Shared WC2026 thresholds live in `dbt/seeds/polymarket_wc2026_contract.csv`;
  dbt models/tests read that seed and Python parity tests assert the Dagster
  defaults match it.
- Public Polymarket knockout marts are additionally filtered to teams present in
  `international_results_wc2026_team_status`, with a small alias seed for source
  naming differences such as `USA` -> `United States`. This removes non-team
  aggregate futures and non-participants from the public odds surface.
- WC2026 match/result rows come from
  `https://raw.githubusercontent.com/martj42/international_results/refs/heads/master/results.csv`
  where `tournament = 'FIFA World Cup'` and `match_date` is between
  `2026-06-11` and `2026-07-19`.
- `stage_key` values are `winner`, `final`, `semifinal`, `quarterfinal`,
  `round_of_16`, and `round_of_32`.
- Public knockout odds are normalized to the progression side. Winner/reach markets
  use the Yes token; elimination-framed markets use the No token. `price_represents`
  is fixed to `progression`, and `progression_outcome_label` states the normalized
  outcome represented by the price. For example, a Round-of-32 elimination market
  with `source_outcome_label = 'No'` exposes `not_eliminated_in_round_of_32`, so a
  high price means the team advanced past that round.
- Public knockout marts keep historical closed/resolved rows. `is_live_market`
  means the source market is active, open, and unresolved. `is_active_team_live_market`
  further requires the FIFA result mart to say the team is still alive. On
  `polymarket_wc2026_knockout_markets`, `is_actionable_live_market` is the safest
  current-consumption filter: the team is still alive, the market is live, and the
  latest hourly close is fresh. `source_state_anomaly` marks upstream rows where
  Gamma reports both active and closed; the derived status treats those rows as closed.
- Use `canonical_team_name`, `tournament_status`, `is_still_alive`,
  `eliminated_stage_key`, and `next_stage_key` on Polymarket marts when joining
  odds to real WC2026 team state.
- `polymarket_wc2026_knockout_markets.current_price_status` separates `fresh_live`,
  `stale_live`, `missing_live`, `historical_closed`, `historical_resolved`, and
  `inactive` rows. Live prices are fresh when the latest hourly close is within
  the contract seed freshness window. Stale/missing live DQ findings are only
  actionable for still-alive teams; eliminated teams that Polymarket still marks
  live are emitted as upstream-lag warnings.
- `polymarket_wc2026_knockout_token_hourly_odds` joins the private incremental
  hourly odds fact to the shared knockout classifier, and only exposes the
  contract seed trailing hourly window. The export script supports `--live-only`
  and `--active-teams-only` filters for downstream live views without adding another mart.
- `international_results_wc2026_data_quality` emits a warning when the latest
  fixture/result source load is older than the contract seed freshness window.
- Use `polymarket_wc2026_market_registry_refresh`, `polymarket_wc2026_hourly_odds_ingest`,
  `polymarket_wc2026_dbt_build`, and `polymarket_wc2026_full_pipeline` for WC2026
  Dagster operations.
- Use `polymarket_us_midterms_2026_market_registry_refresh`,
  `polymarket_us_midterms_2026_hourly_odds_ingest`, and
  `polymarket_us_midterms_2026_full_pipeline` for US midterms Dagster operations.
- Use `kalshi_wc2026_market_registry_refresh`, `kalshi_wc2026_hourly_odds_ingest`,
  and `kalshi_wc2026_full_pipeline` for Kalshi WC2026 Dagster operations.
  `kalshi_wc2026_full_pipeline` also runs `international_results_wc2026_match_results_ingest`
  and a scoped dbt build (`+tag:kalshi`, including `international_results` parents).
  `international_results_wc2026_match_results_ingest` refreshes only the FIFA
  World Cup fixture/result source and is included in the Polymarket WC2026 full pipeline.
- Use `wc2026_knockout_match_odds_full_pipeline` for an atomic fixture,
  Polymarket registry/odds, Kalshi registry/candlestick, permanent-fact, neutral
  mart, and observability refresh. Source-specific dbt jobs exclude the neutral
  `cross_domain` models so a one-sided refresh is not presented as atomic.
- `wc2026_knockout_match_odds_hourly_schedule` targets that combined job and is
  stopped unless `WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED=true`.
- `polymarket_wc2026_knockout_token_hourly_odds` remains the public
  progression-side export for downstream knockout probability views.
- `polymarket_wc2026_graph_token_hourly_odds` is the portable graph input. It
  keeps both tokens per market and exposes `is_progression_token`,
  `opposite_clob_token_id`, `canonical_team_name`, `stage_key`, and
  `progression_outcome_label` so `oddsfox-graph` does not infer WC2026 semantics
  from question text when dbt already classified the market.
- After `make prune-odds-history`, `polymarket_wc2026_raw.odds_history` only guarantees the trailing ~365 days of source
  odds points unless you change the retention window.
- `int_polymarket_wc2026_markets` is the canonical market-level WC2026 scope (grain:
  `scope_name`, `market_id`) with the WC2026 contract volume floor applied.

## dbt Checks

`uv run make dbt-build` runs model builds plus generic and singular data tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- WC2026 market scope (`accepted_values` on `scope_name` and knockout `stage_key`).
- Knockout progression-side token selection, including elimination-framed No-token rows.
- Knockout volume floor and trailing hourly window from the WC2026 contract seed.
- Knockout market and current-price status accepted values.
- FIFA World Cup result scope, stage counts, 48-team roster shape, tied knockout
  advancer inference/DQ surfacing, and Polymarket real-team filtering.
- Hard-fail DQ checks for error-severity rows in `polymarket_wc2026_knockout_data_quality`.
- Warn-level DQ checks for actionable stale/missing live odds, eliminated-team
  upstream live lag, unsurfaced source-state or hourly coverage issues, sparse
  stage/team coverage, live-team alignment, and stale fixture/result source loads.
- Observability run health (warn-level: latest run error-token regression and history coverage floor).
- US midterms grain, OHLC bounds, volume floor from
  `polymarket_us_midterms_2026_contract.csv`, and `scope_name` accepted values.
- Kalshi WC2026 grain, OHLC order, progression-side selection, real-team scope,
  and data-quality checks from `kalshi_wc2026_contract.csv`.
- Official knockout match-number/stage relationships, match 103 exclusion,
  unique provider mappings, exact team-to-advance classification, permanent
  incremental hours, dense null preservation, and four-price pivot behavior.

Warn-level observability tests fail softly in `dbt build` output; treat warnings
as operator signals on real warehouses, not hard release blockers when the
disposable fixture is healthy.

## Breaking change: source-first namespace reset

Public mart, asset, job, script, and schema names now use the source-first
`polymarket_wc2026` namespace. Dagster asset keys are hierarchical under
`polymarket/wc2026/...`; jobs, op config keys, scripts, dbt relations, and
DuckDB schemas use flat `polymarket_wc2026_*` names.

There are no compatibility views, env aliases, or migration shims in v0.1.x.
Delete old local warehouse files (`rm oddsfox.duckdb*`) and rerun quickstart
after upgrading from older layouts.

The neutral `wc2026_*` dbt schemas and permanent platform match facts change
the local warehouse layout. v0.1.x provides no compatibility aliases or schema
migration; reset `oddsfox.duckdb*` before rebuilding an older warehouse.

The knockout hourly time-series mart is a dbt view over a private incremental
hourly fact. If an existing local DuckDB warehouse still has deleted broad
public marts or old relation types, reset the warehouse or drop the affected dbt
schemas before rebuilding.
