{{ config(materialized='table', tags=['polygon_settlement']) }}

with candidate_by_proposition as (
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
        ) as invalid_axis_rows
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
    group by proposition_id
),

candidate_distinct_grains as (
    select distinct
        proposition_id,
        settlement_minute_epoch
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
),

candidate_distinct_grain_summary as (
    select count(*) as distinct_minute_grains
    from candidate_distinct_grains
),

candidate_summary as (
    select
        count(*) as actual_minute_rows,
        sum(yes_normalized_fill_count + no_normalized_fill_count)
            as candidate_fill_count,
        count(*) filter (
            where
            yes_observed is null
            or no_observed is null
            or yes_derived_fill_count > yes_normalized_fill_count
            or no_derived_fill_count > no_normalized_fill_count
            or (
                yes_observed
                and (
                    yes_normalized_fill_count <= 0
                    or yes_share_volume <= 0
                    or yes_gross_collateral_volume <= 0
                    or yes_open_price is null
                    or yes_high_price is null
                    or yes_low_price is null
                    or yes_close_price is null
                    or yes_vwap is null
                    or yes_first_settlement_at_utc is null
                    or yes_last_settlement_at_utc is null
                    or yes_first_settlement_at_utc
                    > yes_last_settlement_at_utc
                )
            )
            or (
                no_observed
                and (
                    no_normalized_fill_count <= 0
                    or no_share_volume <= 0
                    or no_gross_collateral_volume <= 0
                    or no_open_price is null
                    or no_high_price is null
                    or no_low_price is null
                    or no_close_price is null
                    or no_vwap is null
                    or no_first_settlement_at_utc is null
                    or no_last_settlement_at_utc is null
                    or no_first_settlement_at_utc
                    > no_last_settlement_at_utc
                )
            )
            or (
                not yes_observed
                and (
                    yes_normalized_fill_count <> 0
                    or yes_derived_fill_count <> 0
                    or yes_share_volume <> 0
                    or yes_gross_collateral_volume <> 0
                    or yes_open_price is not null
                    or yes_high_price is not null
                    or yes_low_price is not null
                    or yes_close_price is not null
                    or yes_vwap is not null
                    or yes_first_settlement_at_utc is not null
                    or yes_last_settlement_at_utc is not null
                )
            )
            or (
                not no_observed
                and (
                    no_normalized_fill_count <> 0
                    or no_derived_fill_count <> 0
                    or no_share_volume <> 0
                    or no_gross_collateral_volume <> 0
                    or no_open_price is not null
                    or no_high_price is not null
                    or no_low_price is not null
                    or no_close_price is not null
                    or no_vwap is not null
                    or no_first_settlement_at_utc is not null
                    or no_last_settlement_at_utc is not null
                )
            )
            or minute_complete is null
            or minute_complete <> (yes_observed and no_observed)
            or coalesce(minute_status, '') <> case
                when yes_observed and no_observed then 'both_observed'
                when yes_observed then 'yes_only'
                when no_observed then 'no_only'
                else 'no_fills'
            end
        ) as invalid_candidate_state_rows
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
),

axis_summary as (
    select
        count(*) filter (
            where
            actual_rows <> expected_rows
            or distinct_minutes <> expected_rows
            or first_minute <> 0
            or last_minute <> expected_rows - 1
            or invalid_axis_rows > 0
        ) as invalid_proposition_axes
    from candidate_by_proposition
)

select
    candidate_summary.*,
    candidate_distinct_grain_summary.*,
    axis_summary.*
from candidate_summary
cross join candidate_distinct_grain_summary
cross join axis_summary
