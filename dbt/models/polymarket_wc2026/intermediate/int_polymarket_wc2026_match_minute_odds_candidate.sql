{{ config(materialized='table', tags=['cross_domain']) }}

with spine as (
    select
        u.*,
        minute_spine.odds_minute_utc,
        cast(epoch(minute_spine.odds_minute_utc) as bigint) as odds_minute_epoch
    from {{ ref('int_polymarket_wc2026_match_market_universe') }} as u
    -- costguard: allow cross-join, one bounded in-game minute range per market.
    cross join unnest(
        generate_series(
            date_trunc('minute', u.game_started_at_utc),
            date_trunc('minute', u.game_finished_at_utc),
            interval '1 minute'
        )
    ) as minute_spine (odds_minute_utc)
),

joined as (
    select
        s.odds_minute_utc,
        s.odds_minute_epoch,
        s.fifa_match_id,
        s.market_id,
        s.condition_id,
        s.selected_market_event_id,
        s.selected_market_event_slug,
        s.primary_timing_event_id,
        s.primary_timing_event_slug,
        s.stage,
        s.group_label,
        s.home_team,
        s.away_team,
        s.international_results_match_id,
        s.results_source_revision,
        s.results_source_payload_sha256,
        s.results_source_loaded_at,
        s.scheduled_kickoff_at_utc,
        s.game_started_at_utc,
        s.game_finished_at_utc,
        s.sports_market_type,
        s.proposition_type,
        s.question,
        s.yes_represents,
        s.no_represents,
        s.yes_team_name,
        s.no_team_name,
        s.yes_clob_token_id,
        s.no_clob_token_id,
        yes_odds.open_price as yes_open_price,
        yes_odds.high_price as yes_high_price,
        yes_odds.low_price as yes_low_price,
        yes_odds.close_price as yes_close_price,
        yes_odds.average_price as yes_average_price,
        yes_odds.first_observed_at as yes_first_observed_at,
        yes_odds.last_observed_at as yes_last_observed_at,
        no_odds.open_price as no_open_price,
        no_odds.high_price as no_high_price,
        no_odds.low_price as no_low_price,
        no_odds.close_price as no_close_price,
        no_odds.average_price as no_average_price,
        no_odds.first_observed_at as no_first_observed_at,
        no_odds.last_observed_at as no_last_observed_at,
        coalesce(yes_odds.observed_points, 0) as yes_observed_points,
        coalesce(no_odds.observed_points, 0) as no_observed_points,
        yes_odds.observed_points is not null as yes_observed,
        no_odds.observed_points is not null as no_observed,
        yes_odds.observed_points is not null
        and no_odds.observed_points is not null as minute_complete
    from spine as s
    left join {{ ref('int_polymarket_wc2026_match_token_minute_odds') }} as yes_odds
        on
            s.yes_clob_token_id = yes_odds.clob_token_id
            and s.odds_minute_epoch = yes_odds.odds_minute_epoch
    left join {{ ref('int_polymarket_wc2026_match_token_minute_odds') }} as no_odds
        on
            s.no_clob_token_id = no_odds.clob_token_id
            and s.odds_minute_epoch = no_odds.odds_minute_epoch
)

select
    joined.*,
    date_diff(
        'second', joined.scheduled_kickoff_at_utc, joined.game_started_at_utc
    ) / 60.0 as game_start_delta_minutes,
    date_diff(
        'second', joined.game_started_at_utc, joined.game_finished_at_utc
    ) / 60.0 as game_window_minutes,
    joined.odds_minute_utc = date_trunc('minute', joined.game_started_at_utc)
        as is_game_start_minute,
    joined.odds_minute_utc = date_trunc('minute', joined.game_finished_at_utc)
        as is_game_finish_minute,
    case
        when joined.minute_complete then 'complete'
        when
            joined.odds_minute_utc = date_trunc('minute', joined.game_started_at_utc)
            and joined.odds_minute_utc = date_trunc('minute', joined.game_finished_at_utc)
            then 'start_and_finish_boundary_incomplete'
        when joined.odds_minute_utc = date_trunc('minute', joined.game_started_at_utc)
            then 'start_boundary_incomplete'
        when joined.odds_minute_utc = date_trunc('minute', joined.game_finished_at_utc)
            then 'finish_boundary_incomplete'
        else 'interior_incomplete'
    end as minute_status,
    case
        when joined.minute_complete then joined.yes_close_price + joined.no_close_price
    end as yes_no_close_sum,
    case
        when joined.minute_complete
            then abs(joined.yes_close_price + joined.no_close_price - 1.0)
    end as yes_no_close_deviation,
    coalesce(
        joined.minute_complete
        and abs(joined.yes_close_price + joined.no_close_price - 1.0) > 0.05,
        false
    ) as pair_price_anomaly
from joined
