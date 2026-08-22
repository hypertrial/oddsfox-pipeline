{{ config(materialized='view') }}

with dense as (
    select * exclude (source_revision)
    from {{ ref('int_polymarket_soccer_match_result_minute_odds') }}
)

select
    dense.*,
    games.observed_minute_coverage_percent,
    games.maximum_consecutive_gap_minutes,
    games.no_observed_minutes,
    games.no_missing_price_minutes,
    games.no_observed_minute_coverage_percent,
    games.no_maximum_consecutive_gap_minutes
from dense
inner join {{ ref('int_polymarket_soccer_match_result_modeling_games') }} as games
    on dense.event_id = games.event_id
