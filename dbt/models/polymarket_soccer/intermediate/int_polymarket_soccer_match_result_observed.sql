{% set state = ref('int_polymarket_soccer_match_result_market_state') %}
{% set cleanup_sql = '' %}
{% if is_incremental() %}
    {% set cleanup_sql %}
        delete from {{ this }} as target
        where not exists (
            select 1 from {{ state }} as market_state
            where market_state.market_id = target.market_id
              and market_state.source_revision = target.source_revision
        )
    {% endset %}
{% endif %}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['market_id', 'odds_minute_epoch'],
    pre_hook=cleanup_sql
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
)

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
