-- Keep hourly mart checks in one pass; generic tests repeatedly rescan this view.
with checked as (
    select
        hourly.market_id,
        hourly.clob_token_id,
        hourly.odds_hour_utc,
        hourly.open_price,
        hourly.high_price,
        hourly.low_price,
        hourly.close_price,
        hourly.avg_price,
        hourly.observed_points,
        exists(
            select 1
            from {{ ref('polymarket_wc2026_markets') }} as polymarket_wc2026_markets
            where polymarket_wc2026_markets.market_id = hourly.market_id
        ) as has_wc2026_market,
        count(*) over (
            partition by hourly.clob_token_id, hourly.odds_hour_utc
        ) as grain_count
    from {{ ref('polymarket_wc2026_token_hourly_odds') }} as hourly
)

select
    market_id,
    clob_token_id,
    odds_hour_utc,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price,
    observed_points,
    grain_count,
    has_wc2026_market
from checked
where
    clob_token_id is null
    or market_id is null
    or odds_hour_utc is null
    or close_price is null
    or observed_points is null
    or not has_wc2026_market
    or (
        clob_token_id is not null
        and odds_hour_utc is not null
        and grain_count > 1
    )
    or open_price < 0
    or open_price > 1
    or high_price < 0
    or high_price > 1
    or low_price < 0
    or low_price > 1
    or close_price < 0
    or close_price > 1
    or avg_price < 0
    or avg_price > 1
