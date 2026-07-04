-- Keep daily mart checks in one pass; generic tests repeatedly rescan this table.
with checked as (
    select
        daily.market_id,
        daily.clob_token_id,
        daily.odds_date_utc,
        daily.open_price,
        daily.high_price,
        daily.low_price,
        daily.close_price,
        daily.avg_price,
        daily.observed_points,
        exists(
            select 1
            from {{ ref('polymarket_wc2026_markets') }} as polymarket_wc2026_markets
            where polymarket_wc2026_markets.market_id = daily.market_id
        ) as has_wc2026_market,
        count(*) over (
            partition by daily.clob_token_id, daily.odds_date_utc
        ) as grain_count
    from {{ ref('polymarket_wc2026_token_daily_odds') }} as daily
)

select
    market_id,
    clob_token_id,
    odds_date_utc,
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
    or odds_date_utc is null
    or close_price is null
    or observed_points is null
    or not has_wc2026_market
    or (
        clob_token_id is not null
        and odds_date_utc is not null
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
