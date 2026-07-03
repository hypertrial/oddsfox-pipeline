{% set current_max_age_hours = var('polymarket_live_current_max_age_hours', 48) %}

with hourly as (
    select
        market_id,
        outcome_index,
        clob_token_id,
        question,
        outcome_label,
        event_slug,
        is_active,
        is_closed,
        market_volume_usd,
        odds_hour_utc,
        odds_hour_epoch,
        open_price,
        high_price,
        low_price,
        close_price,
        avg_price,
        observed_points,
        first_timestamp,
        first_observed_at,
        last_timestamp,
        last_observed_at
    from {{ ref('selected_token_hourly_odds') }}
),

market_token_counts as (
    select
        market_id,
        count(distinct clob_token_id) as expected_tokens
    from hourly
    group by market_id
),

complete_hours as (
    select
        h.market_id,
        h.odds_hour_epoch
    from hourly as h
    inner join market_token_counts as t
        on h.market_id = t.market_id
    group by h.market_id, h.odds_hour_epoch, t.expected_tokens
    having count(distinct h.clob_token_id) = t.expected_tokens
),

latest_complete as (
    select
        market_id,
        max(odds_hour_epoch) as current_hour_epoch
    from complete_hours
    group by market_id
),

global_bounds as (
    select max(odds_hour_epoch) as global_current_hour_epoch
    from hourly
),

live_markets as (
    select c.market_id
    from latest_complete as c
    inner join hourly as h
        on
            c.market_id = h.market_id
            and c.current_hour_epoch = h.odds_hour_epoch
    cross join global_bounds as b
    group by c.market_id, c.current_hour_epoch, b.global_current_hour_epoch
    having
        bool_or(coalesce(h.is_active, false))
        and not bool_or(coalesce(h.is_closed, false))
        and (
            c.current_hour_epoch
            >= b.global_current_hour_epoch
            - ({{ current_max_age_hours }} * 3600)
        )
)

select
    h.market_id,
    h.outcome_index,
    h.clob_token_id,
    h.question,
    h.outcome_label,
    h.event_slug,
    h.is_active,
    h.is_closed,
    h.market_volume_usd,
    h.odds_hour_utc,
    h.odds_hour_epoch,
    h.open_price,
    h.high_price,
    h.low_price,
    h.close_price,
    h.avg_price,
    h.observed_points,
    h.first_timestamp,
    h.first_observed_at,
    h.last_timestamp,
    h.last_observed_at
from hourly as h
inner join live_markets as m
    on h.market_id = m.market_id
