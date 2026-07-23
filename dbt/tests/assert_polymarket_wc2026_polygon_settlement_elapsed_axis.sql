{{ config(tags=['polygon_settlement']) }}

with axis as (
    select
        proposition_id,
        max(case when stage = 'group_stage' then 150 else 210 end)
            as expected_rows,
        count(*) as actual_rows,
        count(distinct elapsed_window_minute) as distinct_minutes,
        min(elapsed_window_minute) as first_minute,
        max(elapsed_window_minute) as last_minute,
        count(*) filter (
            where
            settlement_minute_utc
            <> analysis_window_start_at_utc
            + elapsed_window_minute * interval '1 minute'
            or settlement_minute_utc >= analysis_window_end_at_utc
        ) as invalid_timestamps
    from {{ ref('polymarket_wc2026_polygon_settlement_minute_odds') }}
    group by proposition_id
)

select *
from axis
where
    actual_rows <> expected_rows
    or distinct_minutes <> expected_rows
    or first_minute <> 0
    or last_minute <> expected_rows - 1
    or invalid_timestamps > 0
