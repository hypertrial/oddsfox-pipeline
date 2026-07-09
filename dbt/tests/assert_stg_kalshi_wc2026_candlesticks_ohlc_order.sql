select
    market_ticker,
    hour_start_utc,
    open_price,
    high_price,
    low_price,
    close_price
from {{ ref('stg_kalshi_wc2026_market_candlesticks_hourly') }}
where
    low_price is not null
    and high_price is not null
    and (
        low_price > high_price
        or (open_price is not null and low_price > open_price)
        or (open_price is not null and high_price < open_price)
        or (close_price is not null and low_price > close_price)
        or (close_price is not null and high_price < close_price)
    )
