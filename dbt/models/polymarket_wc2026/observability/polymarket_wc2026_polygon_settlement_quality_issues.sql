{{ config(materialized='table', tags=['polygon_settlement']) }}

with observation_clock as (
    select
        coalesce(
            max(runs.finished_at),
            (
                select max(markets.reviewed_at_utc)
                from {{ ref('stg_polymarket_wc2026_polygon_settlement_markets') }}
                    as markets
            )
        ) as observed_at
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_runs') }} as runs
),

token_warnings as (
    select
        'proposition_no_fills:' || proposition_id as issue_key,
        'warn' as severity,
        'token_coverage' as issue_type,
        proposition_id,
        max(fifa_match_id) as fifa_match_id,
        cast(null as varchar) as token_id,
        cast(null as timestamp) as settlement_minute_utc,
        cast(0 as double) as measured_value,
        1.0 as threshold_value,
        'Neither oriented outcome token has a normalized settlement fill in the analysis window.'
            as issue_detail
    from {{ ref('polymarket_wc2026_polygon_settlement_token_coverage') }}
    group by proposition_id
    having not bool_or(has_any_fill)

    union all

    select
        'token_no_fills:' || proposition_id || ':' || token_side as issue_key,
        'warn' as severity,
        'token_coverage' as issue_type,
        proposition_id,
        fifa_match_id,
        token_id,
        cast(null as timestamp) as settlement_minute_utc,
        cast(normalized_fill_count as double) as measured_value,
        1.0 as threshold_value,
        'The oriented outcome token has no normalized settlement fills in its analysis window.'
            as issue_detail
    from {{ ref('polymarket_wc2026_polygon_settlement_token_coverage') }}
    where not has_any_fill

    union all

    select
        'sparse_minutes:' || proposition_id || ':' || token_side as issue_key,
        'warn' as severity,
        'minute_coverage' as issue_type,
        proposition_id,
        fifa_match_id,
        token_id,
        cast(null as timestamp) as settlement_minute_utc,
        minute_coverage_ratio as measured_value,
        1.0 as threshold_value,
        'Normalized settlement fills do not cover every scheduled analysis minute; empty minutes remain null.'
            as issue_detail
    from {{ ref('polymarket_wc2026_polygon_settlement_token_coverage') }}
    where has_any_fill and observed_minute_buckets < expected_minute_buckets

    union all

    select
        'derived_fills:' || proposition_id || ':' || token_side as issue_key,
        'warn' as severity,
        'derived_fills' as issue_type,
        proposition_id,
        fifa_match_id,
        token_id,
        cast(null as timestamp) as settlement_minute_utc,
        cast(derived_fill_count as double) / nullif(normalized_fill_count, 0)
            as measured_value,
        0.0 as threshold_value,
        'The token includes explicitly derived complementary MINT/MERGE settlement legs.'
            as issue_detail
    from {{ ref('polymarket_wc2026_polygon_settlement_token_coverage') }}
    where derived_fill_count > 0
),

pair_warnings as (
    select
        'pair_price_deviation:' || proposition_id || ':'
        || settlement_minute_epoch as issue_key,
        'warn' as severity,
        'pair_price' as issue_type,
        proposition_id,
        fifa_match_id,
        cast(null as varchar) as token_id,
        settlement_minute_utc,
        abs(yes_close_price + no_close_price - 1.0) as measured_value,
        0.05 as threshold_value,
        'Observed Yes and No settlement closes differ from a unit pair by more than 0.05.'
            as issue_detail
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
    where
        minute_complete
        and abs(yes_close_price + no_close_price - 1.0) > 0.05
),

verification_warnings as (
    select
        'secondary_verification:' || scan_id as issue_key,
        'warn' as severity,
        'verification' as issue_type,
        cast(null as varchar) as proposition_id,
        cast(null as integer) as fifa_match_id,
        cast(null as varchar) as token_id,
        cast(null as timestamp) as settlement_minute_utc,
        cast(null as double) as measured_value,
        cast(null as double) as threshold_value,
        'Secondary RPC verification is advisory and is not in the matched state ('
        || verification_status || ').' as issue_detail
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_runs') }}
    where
        status = 'published'
        and raw_published
        and verification_status <> 'matched'
    qualify row_number() over (
        order by published_at desc nulls last, finished_at desc nulls last, scan_id desc
    ) = 1
),

axis_by_proposition as (
    select
        proposition_id,
        max(fifa_match_id) as fifa_match_id,
        count(*) as row_count,
        count(distinct elapsed_window_minute) as distinct_minute_count,
        min(elapsed_window_minute) as first_minute,
        max(elapsed_window_minute) as last_minute,
        max(
            case when stage = 'group_stage' then 150 else 210 end
        ) as expected_minute_count,
        count(*) filter (
            where
            settlement_minute_utc
            <> analysis_window_start_at_utc
            + elapsed_window_minute * interval '1 minute'
            or settlement_minute_utc >= analysis_window_end_at_utc
        ) as invalid_timestamp_rows
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
    group by proposition_id
),

candidate_errors as (
    select
        'elapsed_axis:' || proposition_id as issue_key,
        'error' as severity,
        'spine' as issue_type,
        proposition_id,
        fifa_match_id,
        cast(null as varchar) as token_id,
        cast(null as timestamp) as settlement_minute_utc,
        cast(row_count as double) as measured_value,
        cast(expected_minute_count as double) as threshold_value,
        'The proposition minute axis is not the exact zero-based half-open 150/210-minute window.'
            as issue_detail
    from axis_by_proposition
    where
        row_count <> expected_minute_count
        or distinct_minute_count <> expected_minute_count
        or first_minute <> 0
        or last_minute <> expected_minute_count - 1
        or invalid_timestamp_rows > 0

    union all

    select
        'invalid_price:' || proposition_id || ':' || settlement_minute_epoch
            as issue_key,
        'error' as severity,
        'price' as issue_type,
        proposition_id,
        fifa_match_id,
        cast(null as varchar) as token_id,
        settlement_minute_utc,
        1.0 as measured_value,
        0.0 as threshold_value,
        'An observed OHLC or VWAP settlement price falls outside [0, 1].'
            as issue_detail
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
    where
        (
            yes_observed
            and (
                yes_open_price not between 0 and 1
                or yes_high_price not between 0 and 1
                or yes_low_price not between 0 and 1
                or yes_close_price not between 0 and 1
                or yes_vwap not between 0 and 1
            )
        )
        or (
            no_observed
            and (
                no_open_price not between 0 and 1
                or no_high_price not between 0 and 1
                or no_low_price not between 0 and 1
                or no_close_price not between 0 and 1
                or no_vwap not between 0 and 1
            )
        )

    union all

    select
        'invalid_ohlc:' || proposition_id || ':' || settlement_minute_epoch
            as issue_key,
        'error' as severity,
        'ohlc' as issue_type,
        proposition_id,
        fifa_match_id,
        cast(null as varchar) as token_id,
        settlement_minute_utc,
        1.0 as measured_value,
        0.0 as threshold_value,
        'An observed settlement open, close, or VWAP falls outside its low/high range.'
            as issue_detail
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
    where
        (
            yes_observed
            and (
                yes_low_price > yes_high_price
                or yes_open_price not between yes_low_price and yes_high_price
                or yes_close_price not between yes_low_price and yes_high_price
                or yes_vwap not between yes_low_price and yes_high_price
            )
        )
        or (
            no_observed
            and (
                no_low_price > no_high_price
                or no_open_price not between no_low_price and no_high_price
                or no_close_price not between no_low_price and no_high_price
                or no_vwap not between no_low_price and no_high_price
            )
        )
),

all_issues as (
    select * from token_warnings
    union all
    select * from pair_warnings
    union all
    select * from verification_warnings
    union all
    select * from candidate_errors
)

select
    issues.*,
    clock.observed_at
from all_issues as issues
cross join observation_clock as clock
