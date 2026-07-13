with source_markets as (
    select
        market_ticker,
        event_ticker,
        series_ticker,
        title,
        subtitle,
        yes_sub_title,
        no_sub_title,
        status,
        market_type,
        open_time,
        close_time,
        expiration_time,
        occurrence_datetime,
        volume,
        open_interest,
        last_price_dollars,
        scraped_at
    from {{ source('kalshi_wc2026_raw', 'markets') }}
)

select
    market_ticker,
    event_ticker,
    title,
    subtitle,
    yes_sub_title,
    no_sub_title,
    status,
    market_type,
    open_time,
    close_time,
    expiration_time,
    occurrence_datetime,
    volume,
    open_interest,
    scraped_at,
    coalesce(
        nullif(series_ticker, ''),
        split_part(market_ticker, '-', 1)
    ) as series_ticker,
    case
        when position('-' in event_ticker) > 0
            then substring(event_ticker from position('-' in event_ticker) + 1)
    end as event_suffix,
    case
        when market_ticker like event_ticker || '-%'
            then substring(market_ticker from length(event_ticker) + 2)
        when length(market_ticker) - length(replace(market_ticker, '-', '')) >= 2
            then split_part(market_ticker, '-', 3)
    end as market_suffix,
    try_cast(last_price_dollars as double) as last_price
from source_markets
