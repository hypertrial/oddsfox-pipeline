with contract as (
    select *
    from {{ ref('kalshi_wc2026_contract') }}
    where scope_name = 'wc2026'
),

raw_stage as (
    select
        c.stage_key,
        c.stage_rank,
        c.market_direction,
        c.market_status,
        count(distinct c.market_ticker) as raw_classified_markets,
        min(c.market_volume) as raw_min_volume,
        max(c.market_volume) as raw_max_volume
    from {{ ref('int_kalshi_wc2026_stage_classification') }} as c
    group by 1, 2, 3, 4
),

hourly_by_market as (
    select
        market_ticker,
        min(odds_hour_utc) as first_hour_utc,
        max(odds_hour_utc) as latest_hour_utc,
        count(*) as hourly_rows
    from {{ ref('kalshi_wc2026_stage_market_hourly_odds') }}
    group by 1
),

scoped_stage as (
    select
        s.stage_key,
        s.stage_rank,
        s.market_direction,
        s.market_status,
        count(distinct s.market_ticker) as scoped_markets,
        count(h.market_ticker) as markets_with_hourly_odds,
        count(*) - count(h.market_ticker) as markets_missing_hourly_odds,
        min(s.market_volume) as scoped_min_volume,
        max(s.market_volume) as scoped_max_volume,
        min(h.first_hour_utc) as first_hour_utc,
        max(h.latest_hour_utc) as latest_hour_utc,
        sum(coalesce(h.hourly_rows, 0)) as hourly_rows,
        count(*) * max(contract.hourly_window_hours) as expected_hourly_rows,
        round(sum(coalesce(h.hourly_rows, 0))::double / nullif(count(*), 0), 4)
            as avg_hourly_rows_per_market,
        min(coalesce(h.hourly_rows, 0)) as min_hourly_rows_per_market,
        max(coalesce(h.hourly_rows, 0)) as max_hourly_rows_per_market,
        least(
            round(
                sum(coalesce(h.hourly_rows, 0))::double
                / nullif(count(*) * max(contract.hourly_window_hours), 0),
                6
            ),
            1.0
        ) as hourly_completeness_ratio
    from {{ ref('int_kalshi_wc2026_stage_classification') }} as s
    left join hourly_by_market as h
        on s.market_ticker = h.market_ticker
    -- costguard: allow cross-join, WC2026 contract seed has one row.
    cross join contract
    group by 1, 2, 3, 4
)

select
    r.raw_min_volume,
    r.raw_max_volume,
    s.scoped_min_volume,
    s.scoped_max_volume,
    s.first_hour_utc,
    s.latest_hour_utc,
    coalesce(s.stage_key, r.stage_key) as stage_key,
    coalesce(s.market_direction, r.market_direction) as market_direction,
    coalesce(s.market_status, r.market_status) as market_status,
    coalesce(s.stage_rank, r.stage_rank) as stage_rank,
    coalesce(r.raw_classified_markets, 0) as raw_classified_markets,
    coalesce(s.scoped_markets, 0) as scoped_markets,
    coalesce(s.markets_with_hourly_odds, 0) as markets_with_hourly_odds,
    coalesce(s.markets_missing_hourly_odds, 0) as markets_missing_hourly_odds,
    coalesce(s.hourly_rows, 0) as hourly_rows,
    coalesce(s.expected_hourly_rows, 0) as expected_hourly_rows,
    coalesce(s.avg_hourly_rows_per_market, 0) as avg_hourly_rows_per_market,
    coalesce(s.min_hourly_rows_per_market, 0) as min_hourly_rows_per_market,
    coalesce(s.max_hourly_rows_per_market, 0) as max_hourly_rows_per_market,
    coalesce(s.hourly_completeness_ratio, 0) as hourly_completeness_ratio,
    current_timestamp as observed_at
from scoped_stage as s
full outer join raw_stage as r
    on
        s.stage_key = r.stage_key
        and s.market_direction = r.market_direction
        and s.market_status = r.market_status
