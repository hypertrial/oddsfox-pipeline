{% set observed = ref('int_polymarket_soccer_match_result_observed') %}
{% set cleanup_sql = '' %}
{% if is_incremental() %}
    {% set cleanup_sql %}
        delete from {{ this }} as target
        where not exists (
            select 1 from {{ observed }} as observed_state
            where observed_state.market_id = target.market_id
              and observed_state.source_revision = target.source_revision
        )
    {% endset %}
{% endif %}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['market_id', 'odds_minute_epoch'],
    pre_hook=cleanup_sql
) }}

with markets as (
    select distinct
        source_observed.event_id,
        source_observed.event_slug,
        source_observed.event_title,
        source_observed.competition_label,
        source_observed.series_slugs_json,
        source_observed.market_id,
        source_observed.result_role,
        source_observed.home_team,
        source_observed.away_team,
        source_observed.clob_token_id,
        source_observed.match_started_at_utc,
        source_observed.match_finished_at_utc,
        source_observed.kickoff_source,
        source_observed.timing_status,
        source_observed.timing_confidence,
        source_observed.coverage_tier,
        source_observed.source_revision
    from {{ observed }} as source_observed
    {% if is_incremental() %}
        where not exists (
            select 1 from {{ this }} as existing
            where existing.market_id = source_observed.market_id
              and existing.source_revision = source_observed.source_revision
        )
    {% endif %}
),

spine as (
    select
        markets.*,
        spine_minute.odds_minute_utc,
        cast(epoch(spine_minute.odds_minute_utc) as bigint) as odds_minute_epoch
    from markets
    cross join lateral generate_series(
        date_trunc('minute', markets.match_started_at_utc),
        date_trunc('minute', markets.match_finished_at_utc),
        interval 1 minute
    ) as spine_minute (odds_minute_utc)
),

joined as (
    select
        spine.*,
        observed_row.open_odds as observed_open_odds,
        observed_row.high_odds as observed_high_odds,
        observed_row.low_odds as observed_low_odds,
        observed_row.close_odds as observed_close_odds,
        observed_row.avg_odds as observed_avg_odds,
        observed_row.observed_points,
        observed_row.first_observed_at,
        observed_row.last_observed_at,
        last_value(observed_row.close_odds ignore nulls) over (
            partition by spine.market_id order by spine.odds_minute_epoch
            rows between unbounded preceding and current row
        ) as carried_close_odds,
        last_value(observed_row.last_observed_at ignore nulls) over (
            partition by spine.market_id order by spine.odds_minute_epoch
            rows between unbounded preceding and current row
        ) as latest_observed_at
    from spine
    left join {{ observed }} as observed_row
        on
            spine.market_id = observed_row.market_id
            and spine.odds_minute_epoch = observed_row.odds_minute_epoch
)

select
    * exclude (
        observed_open_odds, observed_high_odds, observed_low_odds,
        observed_close_odds, observed_avg_odds, observed_points,
        first_observed_at, last_observed_at, carried_close_odds,
        latest_observed_at
    ),
    latest_observed_at as last_observed_at,
    observed_close_odds is not null as is_observed,
    case
        when observed_close_odds is not null then observed_open_odds
        else carried_close_odds
    end as open_odds,
    case
        when observed_close_odds is not null then observed_high_odds
        else carried_close_odds
    end as high_odds,
    case
        when observed_close_odds is not null then observed_low_odds
        else carried_close_odds
    end as low_odds,
    coalesce(observed_close_odds, carried_close_odds) as close_odds,
    case
        when observed_close_odds is not null then observed_avg_odds
        else carried_close_odds
    end as avg_odds,
    coalesce(observed_points, 0) as observed_points,
    case
        when latest_observed_at is not null
            then date_diff('minute', latest_observed_at, odds_minute_utc)
    end as minutes_since_observation
from joined
