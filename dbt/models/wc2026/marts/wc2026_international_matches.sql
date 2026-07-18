{{ config(alias='international_matches') }}

with matches as (
    select
        source_match.match_id,
        source_match.match_date,
        source_match.home_team,
        source_match.away_team,
        source_match.home_score,
        source_match.away_score,
        source_match.tournament,
        source_match.city,
        source_match.country,
        source_match.is_neutral_site,
        classification.competition_family,
        classification.confederation_scope,
        classification.notes as tournament_notes,
        shootout.shootout_winner,
        shootout.shootout_first_shooter,
        source_match.source_url,
        source_match.source_row_number,
        source_match.source_row_hash,
        source_match.source_loaded_at,
        coalesce(classification.is_friendly, false) as is_friendly,
        coalesce(classification.is_competitive, false) as is_competitive
    from
        {{ source('international_results_wc2026_raw', 'historical_matches') }}
            as source_match
    left join {{ ref('wc2026_tournament_classification') }} as classification
        on source_match.tournament = classification.tournament
    left join {{ source(
        'international_results_wc2026_raw', 'historical_shootouts'
    ) }} as shootout
        on source_match.match_id = shootout.match_id
),

goal_summary as (
    select
        goal.match_id,
        count(*) as goal_events_count,
        count(distinct goal.scorer) as distinct_scorers_count,
        count(*) filter (where goal.is_own_goal) as own_goal_count,
        count(*) filter (where goal.is_penalty_goal) as penalty_goal_count,
        count(*) filter (
            where
            goal.is_penalty_goal
            and goal.scoring_team = matches.home_team
        ) as home_penalty_goals,
        count(*) filter (
            where
            goal.is_penalty_goal
            and goal.scoring_team = matches.away_team
        ) as away_penalty_goals
    from {{ source(
        'international_results_wc2026_raw', 'historical_goalscorers'
    ) }} as goal
    inner join matches
        on goal.match_id = matches.match_id
    group by goal.match_id
)

select
    matches.*,
    matches.home_score + matches.away_score as total_goals,
    case
        when matches.home_score > matches.away_score then 'win'
        when matches.home_score < matches.away_score then 'loss'
        else 'draw'
    end as home_result,
    case
        when matches.home_score < matches.away_score then 'win'
        when matches.home_score > matches.away_score then 'loss'
        else 'draw'
    end as away_result,
    case
        when matches.home_score is null or matches.away_score is null then null
        when matches.home_score > matches.away_score then matches.home_team
        when matches.away_score > matches.home_score then matches.away_team
        when
            lower(trim(matches.shootout_winner))
            = lower(trim(matches.home_team)) then matches.home_team
        when
            lower(trim(matches.shootout_winner))
            = lower(trim(matches.away_team)) then matches.away_team
    end as winning_team,
    coalesce(goals.goal_events_count, 0) as goal_events_count,
    coalesce(goals.distinct_scorers_count, 0) as distinct_scorers_count,
    coalesce(goals.own_goal_count, 0) as own_goal_count,
    coalesce(goals.penalty_goal_count, 0) as penalty_goal_count,
    coalesce(goals.home_penalty_goals, 0) as home_penalty_goals,
    coalesce(goals.away_penalty_goals, 0) as away_penalty_goals
from matches
left join goal_summary as goals
    on matches.match_id = goals.match_id
