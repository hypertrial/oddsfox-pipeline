{{ config(alias='price_liquidity_current') }}

with prices as (
    select
        clob_token_id,
        arg_max(close_price, odds_hour_epoch) as latest_point_price,
        arg_max(odds_hour_utc, odds_hour_epoch) as latest_point_odds_timestamp,
        max(odds_hour_epoch) as latest_point_odds_timestamp_epoch,
        arg_max(close_price, odds_hour_epoch) as latest_daily_close_price,
        avg(close_price) filter (
            where odds_hour_utc >= current_timestamp - interval '24 hours'
        ) as latest_daily_avg_price,
        max(cast(odds_hour_utc as date)) as latest_daily_odds_date_utc,
        sum(observed_points) as observation_count
    from {{ ref('int_polymarket_wc2026_token_hourly_odds') }}
    group by clob_token_id
)

select
    'polymarket' as venue,
    token.market_id,
    token.outcome_index,
    token.clob_token_id as token_id,
    token.condition_id,
    token.outcome_label as outcome,
    token.question,
    token.event_slug,
    prices.latest_daily_close_price,
    prices.latest_daily_avg_price,
    prices.latest_point_price,
    prices.latest_daily_odds_date_utc,
    prices.latest_point_odds_timestamp,
    prices.latest_point_odds_timestamp_epoch,
    prices.observation_count,
    token.market_volume_usd as volume,
    token.is_active as active,
    token.is_closed as closed
from {{ ref('int_polymarket_wc2026_market_tokens') }} as token
left join prices
    on token.clob_token_id = prices.clob_token_id
