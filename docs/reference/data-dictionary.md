# Data Dictionary

This page is the analyst-facing dictionary for public marts. For formal grains,
scope rules, and data-contract tests, see [Data Contracts](data-contracts.md).

## Core Semantics

- `wc2026_marts.wc2026_knockout_match_hourly_odds` compares exact match-level
  team-advance prices across Polymarket and Kalshi. Extra time and penalties
  count; the final means winning the World Cup.
- Match-mart prices are raw hourly closes. Null means no observation for that
  side/provider/hour; do not forward-fill or renormalize.
- Public Polymarket WC2026 knockout prices are normalized to team progression.
- Winner and reach markets use the Yes token; elimination-framed markets may use
  the No token.
- `price_represents = 'progression'` means price columns represent the normalized
  progression outcome in `progression_outcome_label`.
- `current_price_status` separates `fresh_live`, `stale_live`, `missing_live`,
  `historical_closed`, `historical_resolved`, and `inactive`.
- For current analysis, prefer `is_actionable_live_market` where available.
- Midterms Balance of Power combinations are independent binary markets; do not
  force probabilities across combinations to sum to 1.0.
- Kalshi stage marts expose progression prices separately from raw Yes prices.
  Use `progression_*_price` for team-progression analysis.

## Cross-platform WC2026 Mart

### `wc2026_marts.wc2026_knockout_match_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Compare raw match-advance probabilities for the official home and away teams across both providers. |
| Grain | One row per published `fifa_match_id`, `odds_hour_epoch`. |
| Identifiers | FIFA match numbers 73–102 and 104; match 103 is excluded. Provider identifiers are retained separately. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `kickoff_at_utc`; `is_pre_kickoff` distinguishes pregame hours. |
| Price columns | Four nullable home/away advance closes, one pair per provider. |
| Recommended filters | Use `both_sources_complete` for direct comparisons or provider-specific completeness flags for single-source analysis. |
| Common mistakes | Treating prices as regulation moneylines, using provider home/away order, filling nulls, or normalizing pair sums. |

## Polymarket WC2026 Marts

### `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds`

| Property | Value |
| --- | --- |
| Grain | One row per `(odds_minute_utc, market_id)` |
| Coverage | FIFA match IDs 1–104; 216 group moneylines and 32 knockout advance/win markets |
| Intended use | In-game event studies, backtests, and minute-level market analysis |
| Timing | Primary Gamma event `startTime` through `finishedTimestamp`, boundary minutes inclusive |
| Prices | Raw Yes/No minute OHLC, average, point counts, and first/last observation times |
| Null policy | Dense rows are retained; missing token minutes stay null and are never carried forward |
| Semantics | Group Yes/No is literal; knockout Yes/No is official home/away team orientation |
| Match identity | FIFA numeric ID from the schedule; team names and home/away orientation from the uniquely matched latest international-results row |
| Provenance | Selected market event and separate primary timing event IDs/slugs |

Use `proposition_type`, `yes_represents`, and `no_represents` instead of
inferring meaning from token order. For match 103 the proposition is the
official home team winning third place; for match 104 it is winning the final.

### `polymarket_wc2026_marts.polymarket_wc2026_knockout_markets`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Current snapshot for WC2026 Polymarket progression-side knockout prices. |
| Grain | One row per `clob_token_id`. |
| Identifiers | `market_id`, `clob_token_id`, `condition_id`, `canonical_team_name`, `stage_key`. |
| Time columns | `current_price_hour_utc`, `current_price_hour_epoch`, `current_price_age_hours`, match-status dates. |
| Price columns | `current_price`; semantics are `price_represents = 'progression'`. |
| Recommended filters | Use `is_actionable_live_market` for current live analysis. Use `current_price_status` for freshness buckets. |
| Common joins | Join `canonical_team_name` to `international_results_wc2026_marts.international_results_wc2026_team_status.team_name`. |
| Common mistakes | Treating all rows as live; ignoring `current_price_status`; inferring progression from source question text. |

### `polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Trailing hourly OHLC time series for progression-side WC2026 knockout odds. |
| Grain | One row per `clob_token_id`, `odds_hour_epoch`. |
| Identifiers | `market_id`, `clob_token_id`, `canonical_team_name`, `stage_key`, `progression_outcome_label`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `first_observed_at`, `last_observed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Use `price_represents = 'progression'`; filter by `canonical_team_name`, `stage_key`, and `market_status` for focused analyses. |
| Common joins | Join to `polymarket_wc2026_knockout_markets` on `clob_token_id` for latest price status. |
| Common mistakes | Comparing source Yes/No labels directly without checking progression semantics. |

### `polymarket_wc2026_marts.polymarket_wc2026_knockout_market_tokens`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Token universe and classification for public WC2026 Polymarket progression marts. |
| Grain | One row per `clob_token_id`. |
| Identifiers | `market_id`, `clob_token_id`, `outcome_index`, `canonical_team_name`, `stage_key`. |
| Time columns | Match/team status dates such as `next_match_date` and `latest_completed_match_date`. |
| Price columns | None; this is a classified token dimension. |
| Recommended filters | Use for token membership and classification audits, not as a price fact. |
| Common joins | Join to hourly odds on `clob_token_id`; join to team status on `canonical_team_name`. |
| Common mistakes | Expecting current prices here; use `polymarket_wc2026_knockout_markets` instead. |

### `polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Graph/export analysis that needs both Yes and No tokens for each real-team knockout market. |
| Grain | One row per `market_id`, `clob_token_id`, `odds_hour_epoch`. |
| Identifiers | `market_id`, `clob_token_id`, `opposite_clob_token_id`, `canonical_team_name`, `stage_key`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `first_observed_at`, `last_observed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Use `is_progression_token` when you only want the normalized progression side. |
| Common joins | Join progression-only analysis back to `polymarket_wc2026_knockout_token_hourly_odds` on `clob_token_id` and `odds_hour_epoch`. |
| Common mistakes | Using both tokens as independent progression probabilities; one is the opposite token. |

## International Results WC2026 Marts

### `international_results_wc2026_marts.international_results_wc2026_matches`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Clean FIFA World Cup 2026 fixture and result rows. |
| Grain | One row per `match_id`. |
| Identifiers | `match_id`, `home_team`, `away_team`, `stage_key`. |
| Time columns | `match_date`, `source_loaded_at`. |
| Price columns | None. |
| Recommended filters | Use `match_status = 'completed'` for results; include scheduled rows for future fixtures. |
| Common joins | Join team names to `international_results_wc2026_team_status.team_name`. |
| Common mistakes | Treating tied knockout matches as unresolved without checking `advancing_team` and `advancer_inference_status`. |

### `international_results_wc2026_marts.international_results_wc2026_team_status`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Canonical WC2026 team roster and current tournament status. |
| Grain | One row per `team_name`. |
| Identifiers | `team_name`. |
| Time columns | `next_match_date`, `eliminated_match_date`, `latest_completed_match_date`. |
| Price columns | None. |
| Recommended filters | Use `is_still_alive` for active-team analysis; use `tournament_status` for active, eliminated, champion buckets. |
| Common joins | Join Polymarket/Kalshi `canonical_team_name` to `team_name`. |
| Common mistakes | Joining on source team text instead of canonical team names. |

## Kalshi WC2026 Marts

### `kalshi_wc2026_marts.kalshi_wc2026_stage_markets`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Current Kalshi stage-of-elimination market snapshots normalized to progression semantics. |
| Grain | One row per `market_ticker`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `stage_key`. |
| Time columns | `current_price_hour_utc`, `current_price_hour_epoch`, `current_price_age_hours`, `scraped_at`. |
| Price columns | `progression_price`; use `price_represents = 'progression'`. |
| Recommended filters | Use `is_actionable_live_market` for current live analysis; inspect `current_price_status`. |
| Common joins | Join to team status on `canonical_team_name`. |
| Common mistakes | Using `last_price` as the normalized progression price. |

### `kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Hourly Kalshi stage-of-elimination OHLC odds. |
| Grain | One row per `market_ticker`, `odds_hour_epoch`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `stage_key`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `latest_refreshed_at`. |
| Price columns | Raw Yes prices: `yes_open_price`, `yes_high_price`, `yes_low_price`, `yes_close_price`, `yes_avg_price`; progression prices: `progression_open_price`, `progression_high_price`, `progression_low_price`, `progression_close_price`, `progression_avg_price`. |
| Recommended filters | Use `progression_*_price` for team progression; filter by `stage_key` and `canonical_team_name`. |
| Common joins | Join to `kalshi_wc2026_stage_markets` on `market_ticker` for latest status. |
| Common mistakes | Mixing raw Yes prices with progression prices in one analysis. |

### `kalshi_wc2026_marts.kalshi_wc2026_group_winner_markets`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Current Kalshi group-winner market snapshots. |
| Grain | One row per `market_ticker`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `group_letter`. |
| Time columns | `current_price_hour_utc`, `current_price_hour_epoch`, `current_price_age_hours`, `scraped_at`. |
| Price columns | `group_winner_price`. |
| Recommended filters | Use `is_actionable_live_market` for current live analysis; inspect `current_price_status`. |
| Common joins | Join to team status on `canonical_team_name`. |
| Common mistakes | Treating group-winner prices as stage progression prices. |

### `kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Hourly Kalshi group-winner OHLC odds. |
| Grain | One row per `market_ticker`, `odds_hour_epoch`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `group_letter`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `latest_refreshed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Filter by `group_letter`, `canonical_team_name`, or `market_ticker`. |
| Common joins | Join to `kalshi_wc2026_group_winner_markets` on `market_ticker` for latest status. |
| Common mistakes | Comparing group-winner prices to stage progression prices without labeling the market type. |

## Polymarket US Midterms 2026 Mart

### `polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Hourly OHLC odds for admitted US midterms Polymarket tokens. |
| Grain | One row per `clob_token_id`, `odds_hour_epoch`. |
| Identifiers | `market_id`, `clob_token_id`, `outcome_index`, `outcome_label`, `event_slug`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `first_observed_at`, `last_observed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Filter by `event_slug`, `question`, and `outcome_label`; use `is_active` and `is_closed` when separating live and historical rows. |
| Common joins | Join on `market_id` or `clob_token_id` in downstream notebooks. |
| Common mistakes | Summing Balance of Power combination probabilities as if they were mutually exclusive. |
