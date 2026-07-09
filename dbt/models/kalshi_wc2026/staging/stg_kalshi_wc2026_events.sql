select
    event_ticker,
    series_ticker,
    title,
    sub_title,
    category,
    status,
    open_time,
    close_time,
    scraped_at
from {{ source('kalshi_wc2026_raw', 'events') }}
