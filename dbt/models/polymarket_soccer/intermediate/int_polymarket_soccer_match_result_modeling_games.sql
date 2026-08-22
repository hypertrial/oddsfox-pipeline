{{ config(materialized='table') }}

{% set dense = ref('int_polymarket_soccer_match_result_minute_odds') %}

with gap_groups as (
    select
        event_id,
        market_id,
        is_observed,
        sum(cast(is_observed as integer)) over (
            partition by market_id
            order by odds_minute_epoch
            rows between unbounded preceding and current row
        ) as gap_group
    from {{ dense }}
),

game_gaps as (
    select
        event_id,
        max(gap_minutes) as maximum_consecutive_gap_minutes
    from (
        select
            event_id,
            market_id,
            gap_group,
            count(*) as gap_minutes
        from gap_groups
        where not is_observed
        group by event_id, market_id, gap_group
    ) as gaps
    group by event_id
),

no_gap_groups as (
    select
        event_id,
        market_id,
        is_no_observed,
        sum(cast(is_no_observed as integer)) over (
            partition by market_id
            order by odds_minute_epoch
            rows between unbounded preceding and current row
        ) as gap_group
    from {{ dense }}
),

no_game_gaps as (
    select
        event_id,
        max(gap_minutes) as no_maximum_consecutive_gap_minutes
    from (
        select
            event_id,
            market_id,
            gap_group,
            count(*) as gap_minutes
        from no_gap_groups
        where not is_no_observed
        group by event_id, market_id, gap_group
    ) as gaps
    group by event_id
),

game_coverage as (
    select
        event_id,
        count(*) as expected_minutes,
        count(*) filter (where is_observed) as observed_minutes,
        count(*) filter (where close_odds is null) as missing_price_minutes,
        count(*) filter (where is_no_observed) as no_observed_minutes,
        count(*) filter (where no_close_odds is null) as no_missing_price_minutes,
        count(distinct market_id) as market_count,
        count(distinct result_role) as result_role_count
    from {{ dense }}
    group by event_id
)

select
    coverage.event_id,
    coverage.observed_minutes,
    coverage.expected_minutes,
    100.0 * coverage.observed_minutes / coverage.expected_minutes
        as observed_minute_coverage_percent,
    coalesce(gaps.maximum_consecutive_gap_minutes, 0)
        as maximum_consecutive_gap_minutes,
    coverage.no_observed_minutes,
    coverage.no_missing_price_minutes,
    100.0 * coverage.no_observed_minutes / coverage.expected_minutes
        as no_observed_minute_coverage_percent,
    coalesce(no_gaps.no_maximum_consecutive_gap_minutes, 0)
        as no_maximum_consecutive_gap_minutes
from game_coverage as coverage
left join game_gaps as gaps on coverage.event_id = gaps.event_id
left join no_game_gaps as no_gaps on coverage.event_id = no_gaps.event_id
where
    coverage.market_count = 3
    and coverage.result_role_count = 3
    and coverage.missing_price_minutes = 0
    and coverage.observed_minutes * 100 >= coverage.expected_minutes * 99
    and coalesce(gaps.maximum_consecutive_gap_minutes, 0) <= 3
