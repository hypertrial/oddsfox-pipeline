select
    f.fifa_match_id,
    f.stage_key,
    f.stage_rank,
    f.kickoff_at_utc,
    f.home_team as source_home_team,
    f.away_team as source_away_team,
    f.venue,
    f.match_status,
    f.source_url,
    f.source_line_number,
    f.source_line_hash,
    f.source_loaded_at,
    coalesce(home_alias.canonical_team_name, f.home_team) as home_team,
    coalesce(away_alias.canonical_team_name, f.away_team) as away_team
from {{ source('openfootball_wc2026_raw', 'knockout_fixtures') }} as f
left join {{ ref('international_results_wc2026_team_aliases') }} as home_alias
    on lower(f.home_team) = lower(home_alias.market_team_name)
left join {{ ref('international_results_wc2026_team_aliases') }} as away_alias
    on lower(f.away_team) = lower(away_alias.market_team_name)
