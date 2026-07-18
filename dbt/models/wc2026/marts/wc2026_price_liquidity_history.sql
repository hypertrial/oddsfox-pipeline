{{ config(alias='price_liquidity_history') }}

select
    token.market_id,
    hourly.clob_token_id,
    hourly.clob_token_id as token_id,
    cast(hourly.odds_hour_utc as date) as odds_date_utc,
    arg_max(hourly.close_price, hourly.odds_hour_epoch) as daily_close_price,
    avg(hourly.close_price) as daily_avg_price,
    min(hourly.odds_hour_utc) as first_observed_at,
    max(hourly.odds_hour_utc) as last_observed_at,
    sum(hourly.observed_points) as observation_count
from {{ ref('int_polymarket_wc2026_token_hourly_odds') }} as hourly
inner join {{ ref('int_polymarket_wc2026_market_tokens') }} as token
    on hourly.clob_token_id = token.clob_token_id
group by token.market_id, hourly.clob_token_id, cast(hourly.odds_hour_utc as date)
