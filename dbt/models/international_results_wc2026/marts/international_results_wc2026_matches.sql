with base as (
    select
        match_id,
        match_date,
        stage_key,
        stage_rank,
        home_team,
        away_team,
        home_score,
        away_score,
        tournament,
        city,
        country,
        neutral,
        match_status,
        is_knockout,
        source_url,
        source_row_number,
        source_row_hash,
        source_loaded_at
    from {{ ref('stg_international_results_wc2026_match_results') }}
),

later_knockout_teams as (
    select
        m.match_id,
        m.home_team as inferred_advancing_team
    from base as m
    inner join base as later
        on
            m.stage_rank < later.stage_rank
            and later.is_knockout
            and (m.home_team = later.home_team or m.home_team = later.away_team)
    where
        m.is_knockout
        and m.match_status = 'completed'
        and m.home_score = m.away_score

    union all

    select
        m.match_id,
        m.away_team as inferred_advancing_team
    from base as m
    inner join base as later
        on
            m.stage_rank < later.stage_rank
            and later.is_knockout
            and (m.away_team = later.home_team or m.away_team = later.away_team)
    where
        m.is_knockout
        and m.match_status = 'completed'
        and m.home_score = m.away_score
),

unique_later_knockout_teams as (
    select
        match_id,
        inferred_advancing_team
    from later_knockout_teams
    group by 1, 2
),

later_counts as (
    select
        match_id,
        count(*) as inferred_advancing_team_count,
        min(inferred_advancing_team) as inferred_advancing_team
    from unique_later_knockout_teams
    group by 1
)

select
    b.match_id,
    b.match_date,
    b.stage_key,
    b.stage_rank,
    b.home_team,
    b.away_team,
    b.home_score,
    b.away_score,
    b.tournament,
    b.city,
    b.country,
    b.neutral,
    b.match_status,
    b.is_knockout,
    b.source_url,
    b.source_row_number,
    b.source_row_hash,
    b.source_loaded_at,
    b.home_score = b.away_score and b.match_status = 'completed' as is_tied,
    case
        when b.match_status != 'completed' then null
        when b.home_score > b.away_score then b.home_team
        when b.away_score > b.home_score then b.away_team
    end as winner_team,
    case
        when b.match_status != 'completed' then null
        when not b.is_knockout then null
        when b.home_score > b.away_score then b.home_team
        when b.away_score > b.home_score then b.away_team
        when coalesce(l.inferred_advancing_team_count, 0) = 1 then l.inferred_advancing_team
    end as advancing_team,
    case
        when b.match_status != 'completed' then 'scheduled'
        when not b.is_knockout or b.home_score != b.away_score then 'not_required'
        when coalesce(l.inferred_advancing_team_count, 0) = 1 then 'inferred_from_later_fixture'
        when coalesce(l.inferred_advancing_team_count, 0) = 0 then 'missing_later_fixture'
        else 'ambiguous_later_fixture'
    end as advancer_inference_status
from base as b
left join later_counts as l
    on b.match_id = l.match_id
