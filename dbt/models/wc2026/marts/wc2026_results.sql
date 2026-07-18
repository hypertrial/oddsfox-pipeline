{{ config(alias='results') }}

select
    f.match_id,
    r.home_score,
    r.away_score,
    r.source_url as source_provenance,
    r.source_loaded_at as observed_at,
    case
        when not f.is_knockout then null
        when r.home_score > r.away_score then f.home_team
        when r.away_score > r.home_score then f.away_team
        else r.advancing_team
    end as winner_team,
    case
        when r.match_status = 'completed' then 'final'
        else r.match_status
    end as status
from {{ ref('wc2026_fixtures') }} as f
inner join {{ ref('international_results_wc2026_matches') }} as r
    on
        f.match_date = r.match_date
        and {{ canonical_team_match_key('f.home_team') }}
        = {{ canonical_team_match_key('r.home_team') }}
        and {{ canonical_team_match_key('f.away_team') }}
        = {{ canonical_team_match_key('r.away_team') }}
where r.match_status = 'completed'
