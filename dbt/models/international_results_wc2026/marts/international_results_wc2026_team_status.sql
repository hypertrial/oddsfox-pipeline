with matches as (
    select
        match_id,
        match_date,
        stage_key,
        stage_rank,
        home_team,
        away_team,
        home_score,
        away_score,
        match_status,
        is_knockout,
        advancing_team
    from {{ ref('international_results_wc2026_matches') }}
),

team_rows as (
    select home_team as team_name from matches
    union all
    select away_team as team_name from matches
),

teams as (
    select team_name
    from team_rows
    group by 1
),

team_matches as (
    select
        match_id,
        match_date,
        stage_key,
        stage_rank,
        match_status,
        is_knockout,
        team_name,
        opponent_team,
        goals_for,
        goals_against
    from {{ ref('int_international_results_wc2026_match_teams') }}
),

played as (
    select
        team_name,
        count(*) as matches_played,
        sum(case when goals_for > goals_against then 1 else 0 end) as wins,
        sum(case when goals_for = goals_against then 1 else 0 end) as draws,
        sum(case when goals_for < goals_against then 1 else 0 end) as losses,
        sum(goals_for) as goals_for,
        sum(goals_against) as goals_against,
        max(match_date) as latest_completed_match_date,
        arg_max(stage_key, stage_rank) as latest_completed_stage_key
    from team_matches
    where match_status = 'completed'
    group by 1
),

next_matches as (
    select
        team_name,
        min(match_date) as next_match_date,
        arg_min(stage_key, match_date) as next_stage_key
    from team_matches
    where match_status = 'scheduled'
    group by 1
),

knockout_teams as (
    select distinct team_name
    from team_matches
    where stage_key != 'group_stage'
),

group_state as (
    select
        count(*) = 72
        and sum(case when match_status = 'scheduled' then 1 else 0 end) = 0
            as group_stage_complete
    from matches
    where stage_key = 'group_stage'
),

knockout_eliminations as (
    select
        stage_key as eliminated_stage_key,
        stage_rank as eliminated_stage_rank,
        match_date as eliminated_match_date,
        case
            when home_team != advancing_team then home_team
            else away_team
        end as team_name
    from matches
    where
        is_knockout
        and stage_key != 'third_place'
        and match_status = 'completed'
        and advancing_team is not null
),

group_eliminations as (
    select
        t.team_name,
        'group_stage' as eliminated_stage_key,
        0 as eliminated_stage_rank,
        cast(null as date) as eliminated_match_date
    from teams as t
    cross join group_state as g
    left join knockout_teams as k
        on t.team_name = k.team_name
    where g.group_stage_complete and k.team_name is null
),

eliminations as (
    select
        team_name,
        eliminated_stage_key,
        eliminated_stage_rank,
        eliminated_match_date
    from knockout_eliminations
    union all
    select
        team_name,
        eliminated_stage_key,
        eliminated_stage_rank,
        eliminated_match_date
    from group_eliminations
),

latest_elimination as (
    select
        team_name,
        arg_max(eliminated_stage_key, eliminated_stage_rank) as eliminated_stage_key,
        max(eliminated_match_date) as eliminated_match_date
    from eliminations
    group by 1
),

champions as (
    select advancing_team as team_name
    from matches
    where stage_key = 'final' and match_status = 'completed'
)

select
    t.team_name,
    e.eliminated_stage_key,
    e.eliminated_match_date,
    n.next_match_date,
    n.next_stage_key,
    p.latest_completed_match_date,
    p.latest_completed_stage_key,
    case
        when c.team_name is not null then 'champion'
        when e.team_name is not null then 'eliminated'
        else 'active'
    end as tournament_status,
    c.team_name is null and e.team_name is null as is_still_alive,
    coalesce(p.matches_played, 0) as matches_played,
    coalesce(p.wins, 0) as wins,
    coalesce(p.draws, 0) as draws,
    coalesce(p.losses, 0) as losses,
    coalesce(p.goals_for, 0) as goals_for,
    coalesce(p.goals_against, 0) as goals_against
from teams as t
left join played as p
    on t.team_name = p.team_name
left join next_matches as n
    on t.team_name = n.team_name
left join latest_elimination as e
    on t.team_name = e.team_name
left join champions as c
    on t.team_name = c.team_name
