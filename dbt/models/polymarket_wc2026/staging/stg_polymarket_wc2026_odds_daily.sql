select
    daily.clobtokenid as clob_token_id,
    daily.odds_date_utc,
    daily.open_price,
    daily.high_price,
    daily.low_price,
    daily.close_price,
    daily.avg_price,
    daily.observed_points,
    daily.first_timestamp,
    daily.last_timestamp,
    daily.refreshed_at,
    to_timestamp(daily.first_timestamp) at time zone 'UTC' as first_observed_at,
    to_timestamp(daily.last_timestamp) at time zone 'UTC' as last_observed_at
from {{ source('polymarket_wc2026_raw', 'token_odds_daily') }} as daily
inner join {{ ref('stg_polymarket_wc2026_market_tokens') }} as tokens
    on daily.clobtokenid = tokens.clob_token_id
