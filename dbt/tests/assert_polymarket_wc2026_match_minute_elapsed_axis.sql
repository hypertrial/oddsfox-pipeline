with elapsed_axis_by_market as (
    select
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
    group by market_id
)

select *
from elapsed_axis_by_market
where
    invalid_row_count > 0
    or first_elapsed_minute <> 0
    or final_elapsed_minute <> row_count - 1
    or distinct_minute_count <> row_count
    or final_elapsed_minute <> expected_final_elapsed_minute
