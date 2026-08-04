select
    daily.CLOBTOKENID as CLOB_TOKEN_ID,
    daily.ODDS_DATE_UTC,
    daily.OPEN_PRICE,
    daily.HIGH_PRICE,
    daily.LOW_PRICE,
    daily.CLOSE_PRICE,
    daily.AVG_PRICE,
    daily.OBSERVED_POINTS,
    daily.FIRST_TIMESTAMP,
    daily.LAST_TIMESTAMP,
    daily.REFRESHED_AT,
    to_timestamp(daily.FIRST_TIMESTAMP) as FIRST_OBSERVED_AT,
    to_timestamp(daily.LAST_TIMESTAMP) as LAST_OBSERVED_AT
from {{ source('polymarket_wc2026_raw', 'token_odds_daily') }} as daily
inner join {{ ref('stg_polymarket_wc2026_market_tokens') }} as tokens
    on daily.CLOBTOKENID = tokens.clob_token_id
