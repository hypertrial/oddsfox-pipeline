with markets as (
    select
        market_ticker,
        event_ticker,
        series_ticker,
        event_suffix,
        market_suffix,
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
        last_price,
        scraped_at
    from {{ ref('stg_kalshi_wc2026_markets') }}
),

registry as (
    select
        market_ticker,
        scope_name
    from {{ source('kalshi_wc2026_ops', 'market_scope_registry') }}
    where lower(scope_name) = 'wc2026'
)

select
    markets.market_ticker,
    markets.event_ticker,
    markets.series_ticker,
    markets.event_suffix,
    markets.market_suffix,
    markets.title,
    markets.subtitle,
    markets.yes_sub_title,
    markets.no_sub_title,
    markets.status,
    markets.market_type,
    markets.open_time,
    markets.close_time,
    markets.expiration_time,
    markets.occurrence_datetime,
    markets.volume,
    markets.open_interest,
    markets.last_price,
    markets.scraped_at,
    registry.scope_name
from markets
inner join registry
    on markets.market_ticker = registry.market_ticker
