select
    fifa_match_id,
    stage_key,
    stage_rank,
    kickoff_at_utc,
    home_team,
    away_team,
    source_home_team,
    source_away_team,
    venue,
    match_status,
    source_url,
    source_line_hash,
    source_loaded_at,
    not regexp_matches(home_team, '^[WL][0-9]+$')
    and not regexp_matches(away_team, '^[WL][0-9]+$') as teams_resolved
from {{ ref('stg_openfootball_wc2026_knockout_fixtures') }}
where fifa_match_id <> 103
