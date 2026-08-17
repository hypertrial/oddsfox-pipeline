{{ config(materialized='table') }}

with markets as (
    select distinct
        event_id,
        event_slug,
        event_title,
        competition_label,
        series_slugs_json,
        market_id,
        result_role,
        home_team,
        away_team,
        clob_token_id,
        match_started_at_utc,
        match_finished_at_utc,
        kickoff_source,
        timing_status,
        timing_confidence,
        coverage_tier
    from {{ ref('polymarket_soccer_match_result_minute_odds_observed') }}
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
        observed.open_odds as observed_open_odds,
        observed.high_odds as observed_high_odds,
        observed.low_odds as observed_low_odds,
        observed.close_odds as observed_close_odds,
        observed.avg_odds as observed_avg_odds,
        observed.observed_points,
        observed.first_observed_at,
        observed.last_observed_at,
        last_value(observed.close_odds ignore nulls) over (
            partition by spine.market_id
            order by spine.odds_minute_epoch
            rows between unbounded preceding and current row
        ) as carried_close_odds,
        last_value(observed.last_observed_at ignore nulls) over (
            partition by spine.market_id
            order by spine.odds_minute_epoch
            rows between unbounded preceding and current row
        ) as latest_observed_at
    from spine
    left join {{ ref('polymarket_soccer_match_result_minute_odds_observed') }} as observed
        on
            spine.market_id = observed.market_id
            and spine.odds_minute_epoch = observed.odds_minute_epoch
)

select
    * exclude (
        observed_open_odds,
        observed_high_odds,
        observed_low_odds,
        observed_close_odds,
        observed_avg_odds,
        observed_points,
        first_observed_at,
        last_observed_at,
        carried_close_odds,
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
