{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['market_ticker', 'odds_hour_epoch'],
        on_schema_change='fail',
        post_hook="
            delete from {{ this }}
            where odds_hour_utc < current_timestamp - (
                (
                    select hourly_window_days
                    from {{ ref('kalshi_wc2026_contract') }}
                    where scope_name = 'wc2026'
                ) * interval '1 day'
            )
        ",
    )
}}

with contract as (
    select hourly_window_days
    from {{ ref('kalshi_wc2026_contract') }}
    where scope_name = 'wc2026'
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
    where
        c.close_price is not null
        and c.odds_hour_epoch is not null
        and c.hour_start_utc >= current_timestamp - (
            (select contract.hourly_window_days from contract) * interval '1 day'
        )
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
    refreshed_at is null
    or refreshed_at >= (
        select coalesce(max(latest_refreshed_at), timestamp '1970-01-01')
        from {{ this }}
    ) - interval '2 hour'
{% endif %}
