{{ config(materialized='table', tags=['cross_domain']) }}
-- noqa: disable=AL03

with games as (
    select distinct
        fifa_match_id,
        stage,
        scheduled_kickoff_at_utc,
        game_started_at_utc,
        game_finished_at_utc,
        results_source_revision,
        results_source_payload_sha256,
        date_diff('second', scheduled_kickoff_at_utc, game_started_at_utc) / 60.0
            as game_start_delta_minutes,
        date_diff('second', game_started_at_utc, game_finished_at_utc) / 60.0
            as game_window_minutes
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}
),

pair_price as (
    select
        'pair_price_anomaly:' || market_id || ':' || odds_minute_epoch as issue_key,
        'warn' as severity,
        'pair_price' as issue_type,
        fifa_match_id,
        market_id,
        cast(null as varchar) as clob_token_id,
        odds_minute_epoch,
        yes_no_close_deviation as measured_value,
        0.05 as threshold_value,
        'Raw Yes/No minute closes differ from one by more than 0.05.'
            as issue_detail
    from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
    where pair_price_anomaly
),

interior_gaps as (
    select
        'interior_incomplete:' || market_id || ':' || odds_minute_epoch
            as issue_key,
        'warn' as severity,
        'minute_completeness' as issue_type,
        fifa_match_id,
        market_id,
        cast(null as varchar) as clob_token_id,
        odds_minute_epoch,
        1.0 as measured_value,
        0.0 as threshold_value,
        'One or both token prices are missing from an interior game minute.'
            as issue_detail
    from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
    where minute_status = 'interior_incomplete'
),

token_warnings as (
    select
        'cadence_gap:' || clob_token_id as issue_key,
        'warn' as severity,
        'cadence' as issue_type,
        fifa_match_id,
        market_id,
        clob_token_id,
        cast(null as bigint) as odds_minute_epoch,
        cast(max_observation_gap_seconds as double) as measured_value,
        120.0 as threshold_value,
        'Token history contains an observation gap greater than 120 seconds.'
            as issue_detail
    from {{ ref('polymarket_wc2026_match_minute_token_coverage') }}
    where cadence_gap_warning

    union all

    select
        'first_boundary_offset:' || clob_token_id,
        'warn',
        'cadence',
        fifa_match_id,
        market_id,
        clob_token_id,
        cast(null as bigint),
        cast(first_observation_offset_seconds as double),
        120.0,
        'First token observation is more than 120 seconds after game start.'
    from {{ ref('polymarket_wc2026_match_minute_token_coverage') }}
    where first_boundary_offset_warning

    union all

    select
        'last_boundary_offset:' || clob_token_id,
        'warn',
        'cadence',
        fifa_match_id,
        market_id,
        clob_token_id,
        cast(null as bigint),
        cast(last_observation_offset_seconds as double),
        120.0,
        'Last token observation is more than 120 seconds before game finish.'
    from {{ ref('polymarket_wc2026_match_minute_token_coverage') }}
    where last_boundary_offset_warning

    union all

    select
        'constant_price:' || clob_token_id,
        'warn',
        'cadence',
        fifa_match_id,
        market_id,
        clob_token_id,
        cast(null as bigint),
        cast(distinct_price_count as double),
        1.0,
        'Token has only one distinct in-game source price.'
    from {{ ref('polymarket_wc2026_match_minute_token_coverage') }}
    where constant_price_warning
),

timing_warnings as (
    select
        'kickoff_shift:' || fifa_match_id as issue_key,
        'warn' as severity,
        'timing' as issue_type,
        fifa_match_id,
        cast(null as varchar) as market_id,
        cast(null as varchar) as clob_token_id,
        cast(null as bigint) as odds_minute_epoch,
        cast(abs(game_start_delta_minutes) as double) as measured_value,
        60.0 as threshold_value,
        'Actual Gamma start differs from scheduled kickoff by more than 60 minutes.'
            as issue_detail
    from games
    where abs(game_start_delta_minutes) > 60

    union all

    select
        'game_window:' || fifa_match_id,
        'warn',
        'timing',
        fifa_match_id,
        cast(null as varchar),
        cast(null as varchar),
        cast(null as bigint),
        cast(game_window_minutes as double),
        case when stage = 'group_stage' then 150.0 else 210.0 end,
        'Actual Gamma game window exceeds the calibrated stage threshold.'
    from games
    where
        (stage = 'group_stage' and game_window_minutes > 150)
        or (stage <> 'group_stage' and game_window_minutes > 210)
),

elapsed_axis_by_market as (
    select
        fifa_match_id,
        market_id,
        count(*) as row_count,
        count(distinct elapsed_window_minute) as distinct_minute_count,
        min(elapsed_window_minute) as first_elapsed_minute,
        max(elapsed_window_minute) as final_elapsed_minute,
        max(
            date_diff(
                'minute',
                date_trunc('minute', game_started_at_utc),
                date_trunc('minute', game_finished_at_utc)
            )
        ) as expected_final_elapsed_minute,
        count(*) filter (
            where
            elapsed_window_minute is null
            or elapsed_window_minute < 0
            or elapsed_window_minute <> date_diff(
                'minute',
                date_trunc('minute', game_started_at_utc),
                odds_minute_utc
            )
        ) as invalid_row_count
    from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
    group by fifa_match_id, market_id
),

elapsed_axis_validation as (
    select
        *,
        greatest(
            invalid_row_count,
            abs(row_count - distinct_minute_count),
            coalesce(abs(first_elapsed_minute), 1),
            coalesce(
                abs(
                    final_elapsed_minute
                    - first_elapsed_minute
                    + 1
                    - distinct_minute_count
                ),
                1
            ),
            coalesce(
                abs(final_elapsed_minute - expected_final_elapsed_minute),
                1
            )
        ) as invalid_axis_row_count
    from elapsed_axis_by_market
),

elapsed_axis_errors as (
    select
        'elapsed_axis:' || market_id as issue_key,
        'error' as severity,
        'spine' as issue_type,
        fifa_match_id,
        market_id,
        cast(null as varchar) as clob_token_id,
        cast(null as bigint) as odds_minute_epoch,
        cast(invalid_axis_row_count as double) as measured_value,
        0.0 as threshold_value,
        'Elapsed window minutes are invalid, non-contiguous, or inconsistent with UTC game-window buckets.'
            as issue_detail
    from elapsed_axis_validation
    where
        invalid_row_count > 0
        or first_elapsed_minute <> 0
        or final_elapsed_minute <> row_count - 1
        or distinct_minute_count <> row_count
        or final_elapsed_minute <> expected_final_elapsed_minute
),

candidate_errors as (
    select
        'invalid_price:' || market_id || ':' || odds_minute_epoch as issue_key,
        'error' as severity,
        'price' as issue_type,
        fifa_match_id,
        market_id,
        cast(null as varchar) as clob_token_id,
        odds_minute_epoch,
        1.0 as measured_value,
        0.0 as threshold_value,
        'One or more observed OHLC/average probabilities fall outside [0, 1].'
            as issue_detail
    from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
    where
        (
            yes_observed
            and (
                yes_open_price not between 0 and 1
                or yes_high_price not between 0 and 1
                or yes_low_price not between 0 and 1
                or yes_close_price not between 0 and 1
                or yes_average_price not between 0 and 1
            )
        )
        or (
            no_observed
            and (
                no_open_price not between 0 and 1
                or no_high_price not between 0 and 1
                or no_low_price not between 0 and 1
                or no_close_price not between 0 and 1
                or no_average_price not between 0 and 1
            )
        )

    union all

    select
        'invalid_ohlc:' || market_id || ':' || odds_minute_epoch,
        'error',
        'ohlc',
        fifa_match_id,
        market_id,
        cast(null as varchar),
        odds_minute_epoch,
        1.0,
        0.0,
        'One or more observed OHLC/average values violate ordering invariants.'
    from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
    where
        (
            yes_observed
            and (
                yes_low_price > yes_high_price
                or yes_open_price not between yes_low_price and yes_high_price
                or yes_close_price not between yes_low_price and yes_high_price
                or yes_average_price not between yes_low_price and yes_high_price
            )
        )
        or (
            no_observed
            and (
                no_low_price > no_high_price
                or no_open_price not between no_low_price and no_high_price
                or no_close_price not between no_low_price and no_high_price
                or no_average_price not between no_low_price and no_high_price
            )
        )
),

structural_errors as (
    select
        'fetch_not_published:' || coalesce(latest_fetch_run_id, 'missing')
        || ':' || clob_token_id as issue_key,
        'error' as severity,
        'fetch' as issue_type,
        fifa_match_id,
        market_id,
        clob_token_id,
        cast(null as bigint) as odds_minute_epoch,
        cast(coalesce(raw_observation_count, 0) as double) as measured_value,
        1.0 as threshold_value,
        'Latest token fetch is missing, unsuccessful, or not atomically published.'
            as issue_detail
    from {{ ref('polymarket_wc2026_match_minute_token_coverage') }}
    where
        latest_fetch_run_id is null
        or latest_fetch_status <> 'success'
        or not coalesce(latest_fetch_raw_published, false)

    union all

    select
        'missing_token_history:' || clob_token_id,
        'error',
        'history',
        fifa_match_id,
        market_id,
        clob_token_id,
        cast(null as bigint),
        cast(raw_observation_count as double),
        1.0,
        'Mapped token has no exact-window raw observation history.'
    from {{ ref('polymarket_wc2026_match_minute_token_coverage') }}
    where raw_observation_count = 0

    union all

    select
        'results_provenance:' || fifa_match_id,
        'error',
        'provenance',
        fifa_match_id,
        cast(null as varchar),
        cast(null as varchar),
        cast(null as bigint),
        0.0,
        1.0,
        'Game is missing a valid immutable results revision or payload SHA-256.'
    from games
    where
        not regexp_full_match(coalesce(results_source_revision, ''), '[0-9a-f]{40}')
        or not regexp_full_match(
            coalesce(results_source_payload_sha256, ''), '[0-9a-f]{64}'
        )
)

select
    *,
    current_timestamp as observed_at
from pair_price
union all
select
    *,
    current_timestamp
from interior_gaps
union all
select
    *,
    current_timestamp
from token_warnings
union all
select
    *,
    current_timestamp
from timing_warnings
union all
select
    *,
    current_timestamp
from elapsed_axis_errors
union all
select
    *,
    current_timestamp
from candidate_errors
union all
select
    *,
    current_timestamp
from structural_errors
