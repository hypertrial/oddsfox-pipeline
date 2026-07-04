select
    match_id,
    match_date,
    stage_key,
    stage_rank,
    match_status,
    is_knockout,
    home_team as team_name,
    away_team as opponent_team,
    home_score as goals_for,
    away_score as goals_against
from {{ ref('stg_international_results_wc2026_match_results') }}

union all

select
    match_id,
    match_date,
    stage_key,
    stage_rank,
    match_status,
    is_knockout,
    away_team as team_name,
    home_team as opponent_team,
    away_score as goals_for,
    home_score as goals_against
from {{ ref('stg_international_results_wc2026_match_results') }}
