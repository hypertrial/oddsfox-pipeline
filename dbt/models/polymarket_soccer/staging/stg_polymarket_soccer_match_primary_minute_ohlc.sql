select
    market_id,
    clob_token_id,
    odds_minute_epoch,
    odds_minute_utc,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price,
    observed_points,
    first_observed_at,
    last_observed_at
from {{ source('polymarket_soccer_raw', 'match_primary_minute_ohlc') }}
