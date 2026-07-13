{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['market_ticker', 'odds_hour_epoch'],
        on_schema_change='fail',
    )
}}

with eligible_markets as (
    select market_ticker
    from {{ ref('int_kalshi_wc2026_match_advance_markets') }}
    where not is_ambiguous_mapping
),

source_candles as (
    select
        c.market_ticker,
        c.hour_start_utc as odds_hour_utc,
        c.odds_hour_epoch,
        c.open_price,
        c.high_price,
        c.low_price,
        c.close_price,
        c.avg_price,
        c.volume,
        c.refreshed_at
    from {{ ref('stg_kalshi_wc2026_market_candlesticks_hourly') }} as c
    inner join eligible_markets as m
        on c.market_ticker = m.market_ticker
    where c.close_price is not null and c.odds_hour_epoch is not null
)

select
    market_ticker,
    odds_hour_utc,
    odds_hour_epoch,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price,
    volume,
    refreshed_at as latest_refreshed_at
from source_candles
{% if is_incremental() %}
where
    source_candles.refreshed_at is null
    or source_candles.refreshed_at >= (
        select coalesce(max(latest_refreshed_at), timestamp '1970-01-01')
        from {{ this }}
    ) - interval '2 hour'
    or not exists (
        select 1
        from {{ this }} as existing
        where existing.market_ticker = source_candles.market_ticker
    )
{% endif %}
