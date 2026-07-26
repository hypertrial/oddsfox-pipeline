{{ config(alias='team_ratings_pre_match') }}

-- Grain: one row per (match, team). Pre-match Elo is the published reverse of
-- EloRatings' post rating and rating change (home: post - change, away: post + change).
-- Ceiling: only completed matches EloRatings published; no future fixtures.

with latest_results as (
    select *
    from {{ source('wc2026_canonical_raw', 'eloratings__match_results') }}
    where _snapshot_id = {{ latest_wc2026_snapshot_id('eloratings') }}
),

team_rows as (
    select
        match_date,
        competition,
        home_team_code as team_code,
        home_team_name as team_name,
        away_team_code as opponent_code,
        away_team_name as opponent_name,
        true as is_home,
        home_post_rating - rating_change as pre_match_rating,
        home_post_rating as post_match_rating,
        rating_change,
        home_goals as goals_for,
        away_goals as goals_against,
        _snapshot_id as snapshot_id,
        _collected_at as collected_at
    from latest_results

    union all

    select
        match_date,
        competition,
        away_team_code as team_code,
        away_team_name as team_name,
        home_team_code as opponent_code,
        home_team_name as opponent_name,
        false as is_home,
        away_post_rating + rating_change as pre_match_rating,
        away_post_rating as post_match_rating,
        -rating_change as rating_change,
        away_goals as goals_for,
        home_goals as goals_against,
        _snapshot_id as snapshot_id,
        _collected_at as collected_at
    from latest_results
)

select
    match_date,
    competition,
    team_code,
    team_name,
    opponent_code,
    opponent_name,
    is_home,
    pre_match_rating,
    post_match_rating,
    rating_change,
    goals_for,
    goals_against,
    snapshot_id,
    collected_at
from team_rows
where match_date >= date '2026-01-01'
