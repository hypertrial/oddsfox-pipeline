select
    event_id,
    market_id,
    result_role,
    home_team,
    away_team,
    yes_token_id,
    no_token_id,
    window_start_at,
    window_end_at,
    kickoff_source,
    timing_status,
    timing_confidence,
    coverage_tier,
    refreshed_at
from {{ source('polymarket_soccer_ops', 'match_result_registry') }}
