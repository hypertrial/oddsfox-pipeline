select
    market_ticker,
    hour_start_utc,
    cast(epoch(hour_start_utc) as bigint) as odds_hour_epoch,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price,
    volume,
    refreshed_at
from {{ source('kalshi_wc2026_raw', 'market_candlesticks_hourly') }}
