{{ config(materialized='table') }}

with mapped as (
    select
        registry.*,
        events.event_slug,
        events.event_title,
        events.event_subtitle,
        events.series_slugs_json
    from {{ ref('stg_polymarket_soccer_match_result_registry') }} as registry
    inner join {{ ref('stg_polymarket_soccer_event_latest') }} as events
        on registry.event_id = events.event_id
)

select
    event_id,
    max(event_slug) as event_slug,
    max(event_title) as event_title,
    max(event_subtitle) as competition_label,
    max(series_slugs_json) as series_slugs_json,
    max(home_team) as home_team,
    max(away_team) as away_team,
    max(window_start_at) as match_started_at_utc,
    max(window_end_at) as match_finished_at_utc,
    max(kickoff_source) as kickoff_source,
    max(timing_status) as timing_status,
    max(timing_confidence) as timing_confidence,
    max(coverage_tier) as coverage_tier,
    max(case when result_role = 'home_win' then market_id end) as home_win_market_id,
    max(case when result_role = 'draw' then market_id end) as draw_market_id,
    max(case when result_role = 'away_win' then market_id end) as away_win_market_id
from mapped
group by event_id
having count(*) = 3 and count(distinct result_role) = 3
