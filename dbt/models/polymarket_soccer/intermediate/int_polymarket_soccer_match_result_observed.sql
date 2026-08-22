{% set state = ref('int_polymarket_soccer_match_result_market_state') %}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['market_id', 'odds_minute_epoch'],
    post_hook="
        delete from {{ this }} as target
        where not exists (
            select 1
            from {{ ref('int_polymarket_soccer_match_result_market_state') }} as market_state
            where market_state.market_id = target.market_id
              and market_state.source_revision = target.source_revision
        )
    "
) }}

with dirty_markets as (
    select market_state.*
    from {{ state }} as market_state
    {% if is_incremental() %}
        where not exists (
            select 1 from {{ this }} as existing
            where existing.market_id = market_state.market_id
              and existing.source_revision = market_state.source_revision
        )
    {% endif %}
),

yes_ticks as (
    select
        dirty.event_id,
        dirty.event_slug,
        dirty.event_title,
        dirty.event_subtitle as competition_label,
        dirty.series_slugs_json,
        dirty.market_id,
        dirty.result_role,
        dirty.home_team,
        dirty.away_team,
        dirty.yes_token_id as clob_token_id,
        dirty.no_token_id,
        dirty.window_start_at as match_started_at_utc,
        dirty.window_end_at as match_finished_at_utc,
        dirty.kickoff_source,
        dirty.timing_status,
        dirty.timing_confidence,
        dirty.coverage_tier,
        odds.odds_minute_epoch,
        odds.odds_minute_utc,
        odds.open_price as open_odds,
        odds.high_price as high_odds,
        odds.low_price as low_odds,
        odds.close_price as close_odds,
        odds.avg_price as avg_odds,
        odds.observed_points,
        odds.first_observed_at,
        odds.last_observed_at,
        dirty.source_revision
    from dirty_markets as dirty
    inner join {{ ref('stg_polymarket_soccer_match_primary_minute_ohlc') }} as odds
        on
            dirty.market_id = odds.market_id
            and dirty.yes_token_id = odds.clob_token_id
    where
        odds.odds_minute_utc >= date_trunc('minute', dirty.window_start_at)
        and odds.odds_minute_utc <= date_trunc('minute', dirty.window_end_at)
),

no_ticks as (
    select
        dirty.event_id,
        dirty.event_slug,
        dirty.event_title,
        dirty.event_subtitle as competition_label,
        dirty.series_slugs_json,
        dirty.market_id,
        dirty.result_role,
        dirty.home_team,
        dirty.away_team,
        dirty.yes_token_id as clob_token_id,
        dirty.no_token_id,
        dirty.window_start_at as match_started_at_utc,
        dirty.window_end_at as match_finished_at_utc,
        dirty.kickoff_source,
        dirty.timing_status,
        dirty.timing_confidence,
        dirty.coverage_tier,
        odds.odds_minute_epoch,
        odds.odds_minute_utc,
        odds.open_price as no_open_odds,
        odds.high_price as no_high_odds,
        odds.low_price as no_low_odds,
        odds.close_price as no_close_odds,
        odds.avg_price as no_avg_odds,
        odds.observed_points as no_observed_points,
        odds.first_observed_at as no_first_observed_at,
        odds.last_observed_at as no_last_observed_at,
        dirty.source_revision
    from dirty_markets as dirty
    inner join {{ ref('stg_polymarket_soccer_match_primary_minute_ohlc') }} as odds
        on
            dirty.market_id = odds.market_id
            and dirty.no_token_id = odds.clob_token_id
    where
        odds.odds_minute_utc >= date_trunc('minute', dirty.window_start_at)
        and odds.odds_minute_utc <= date_trunc('minute', dirty.window_end_at)
),

minutes as (
    select market_id, odds_minute_epoch from yes_ticks
    union
    select market_id, odds_minute_epoch from no_ticks
)

select
    coalesce(yes_ticks.event_id, no_ticks.event_id) as event_id,
    coalesce(yes_ticks.event_slug, no_ticks.event_slug) as event_slug,
    coalesce(yes_ticks.event_title, no_ticks.event_title) as event_title,
    coalesce(yes_ticks.competition_label, no_ticks.competition_label)
        as competition_label,
    coalesce(yes_ticks.series_slugs_json, no_ticks.series_slugs_json)
        as series_slugs_json,
    minutes.market_id,
    coalesce(yes_ticks.result_role, no_ticks.result_role) as result_role,
    coalesce(yes_ticks.home_team, no_ticks.home_team) as home_team,
    coalesce(yes_ticks.away_team, no_ticks.away_team) as away_team,
    coalesce(yes_ticks.clob_token_id, no_ticks.clob_token_id) as clob_token_id,
    coalesce(yes_ticks.no_token_id, no_ticks.no_token_id) as no_token_id,
    coalesce(yes_ticks.match_started_at_utc, no_ticks.match_started_at_utc)
        as match_started_at_utc,
    coalesce(yes_ticks.match_finished_at_utc, no_ticks.match_finished_at_utc)
        as match_finished_at_utc,
    coalesce(yes_ticks.kickoff_source, no_ticks.kickoff_source) as kickoff_source,
    coalesce(yes_ticks.timing_status, no_ticks.timing_status) as timing_status,
    coalesce(yes_ticks.timing_confidence, no_ticks.timing_confidence)
        as timing_confidence,
    coalesce(yes_ticks.coverage_tier, no_ticks.coverage_tier) as coverage_tier,
    minutes.odds_minute_epoch,
    coalesce(yes_ticks.odds_minute_utc, no_ticks.odds_minute_utc)
        as odds_minute_utc,
    yes_ticks.open_odds,
    yes_ticks.high_odds,
    yes_ticks.low_odds,
    yes_ticks.close_odds,
    yes_ticks.avg_odds,
    coalesce(yes_ticks.observed_points, 0) as observed_points,
    yes_ticks.first_observed_at,
    yes_ticks.last_observed_at,
    no_ticks.no_open_odds,
    no_ticks.no_high_odds,
    no_ticks.no_low_odds,
    no_ticks.no_close_odds,
    no_ticks.no_avg_odds,
    coalesce(no_ticks.no_observed_points, 0) as no_observed_points,
    no_ticks.no_first_observed_at,
    no_ticks.no_last_observed_at,
    coalesce(yes_ticks.source_revision, no_ticks.source_revision) as source_revision
from minutes
left join yes_ticks
    on
        minutes.market_id = yes_ticks.market_id
        and minutes.odds_minute_epoch = yes_ticks.odds_minute_epoch
left join no_ticks
    on
        minutes.market_id = no_ticks.market_id
        and minutes.odds_minute_epoch = no_ticks.odds_minute_epoch
