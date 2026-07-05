with extracted as (
    select
        m.market_id,
        m.question,
        m.category,
        m.description,
        m.outcomes,
        m.volume as market_volume_usd,
        m.is_active,
        m.is_closed,
        m.created_at,
        m.scraped_at,
        m.end_date,
        m.slug as market_slug,
        m.event_slug,
        m.event_id,
        m.condition_id,
        m.sports_market_type,
        m.game_start_time,
        m.group_item_title,
        m.tags,
        m.clob_token_ids,
        m.is_resolved,
        m.winning_outcome,
        m.winning_clob_token_id,
        case
            when m.question like 'Will % win the 2026 FIFA World Cup?'
                then replace(replace(m.question, 'Will ', ''), ' win the 2026 FIFA World Cup?', '')
            else ''
        end as winner_team,
        case
            when m.question like 'Will % reach the 2026 FIFA World Cup final?'
                then replace(replace(m.question, 'Will ', ''), ' reach the 2026 FIFA World Cup final?', '')
            else ''
        end as final_team,
        case
            when m.question like 'Will % reach the Semifinals at the 2026 FIFA World Cup?'
                then replace(replace(m.question, 'Will ', ''), ' reach the Semifinals at the 2026 FIFA World Cup?', '')
            else ''
        end as semifinal_team,
        case
            when m.question like 'Will % reach the Quarterfinals at the 2026 FIFA World Cup?'
                then replace(replace(m.question, 'Will ', ''), ' reach the Quarterfinals at the 2026 FIFA World Cup?', '')
            else ''
        end as quarterfinal_team,
        case
            when m.question like 'Will % reach the Round of 16 at the 2026 FIFA World Cup?'
                then replace(replace(m.question, 'Will ', ''), ' reach the Round of 16 at the 2026 FIFA World Cup?', '')
            else ''
        end as round_of_16_reach_team,
        case
            when m.question like 'Will % be eliminated in the Round of 16 of the World Cup?'
                then replace(replace(m.question, 'Will ', ''), ' be eliminated in the Round of 16 of the World Cup?', '')
            else ''
        end as round_of_16_elimination_team,
        case
            when m.question like 'Will % reach the Round of 32 at the 2026 FIFA World Cup?'
                then replace(replace(m.question, 'Will ', ''), ' reach the Round of 32 at the 2026 FIFA World Cup?', '')
            else ''
        end as round_of_32_reach_team,
        case
            when m.question like 'Will % be eliminated in the Round of 32 of the World Cup?'
                then replace(replace(m.question, 'Will ', ''), ' be eliminated in the Round of 32 of the World Cup?', '')
            else ''
        end as round_of_32_elimination_team
    from {{ ref('stg_polymarket_wc2026_markets') }} as m
),

classified as (
    select
        market_id,
        question,
        category,
        description,
        outcomes,
        market_volume_usd,
        is_active,
        is_closed,
        created_at,
        scraped_at,
        end_date,
        market_slug,
        event_slug,
        event_id,
        condition_id,
        sports_market_type,
        game_start_time,
        group_item_title,
        tags,
        clob_token_ids,
        is_resolved,
        winning_outcome,
        winning_clob_token_id,
        'progression' as price_represents,
        case
            when winner_team != '' then 'winner'
            when final_team != '' then 'final'
            when semifinal_team != '' then 'semifinal'
            when quarterfinal_team != '' then 'quarterfinal'
            when round_of_16_reach_team != '' or round_of_16_elimination_team != '' then 'round_of_16'
            when round_of_32_reach_team != '' or round_of_32_elimination_team != '' then 'round_of_32'
        end as stage_key,
        case
            when winner_team != '' then 5
            when final_team != '' then 4
            when semifinal_team != '' then 3
            when quarterfinal_team != '' then 2
            when round_of_16_reach_team != '' or round_of_16_elimination_team != '' then 1
            when round_of_32_reach_team != '' or round_of_32_elimination_team != '' then 0
        end as stage_rank,
        case
            when winner_team != '' then 'winner'
            when
                final_team != ''
                or semifinal_team != ''
                or quarterfinal_team != ''
                or round_of_16_reach_team != ''
                or round_of_32_reach_team != ''
                then 'advance'
            when
                round_of_16_elimination_team != ''
                or round_of_32_elimination_team != ''
                then 'elimination'
        end as market_direction,
        case
            when winner_team != '' then 'win_world_cup'
            when final_team != '' then 'reach_final'
            when semifinal_team != '' then 'reach_semifinal'
            when quarterfinal_team != '' then 'reach_quarterfinal'
            when round_of_16_reach_team != '' then 'reach_round_of_16'
            when round_of_16_elimination_team != '' then 'not_eliminated_in_round_of_16'
            when round_of_32_reach_team != '' then 'reach_round_of_32'
            when round_of_32_elimination_team != '' then 'not_eliminated_in_round_of_32'
        end as progression_outcome_label,
        coalesce(
            nullif(winner_team, ''),
            nullif(final_team, ''),
            nullif(semifinal_team, ''),
            nullif(quarterfinal_team, ''),
            nullif(round_of_16_reach_team, ''),
            nullif(round_of_16_elimination_team, ''),
            nullif(round_of_32_reach_team, ''),
            nullif(round_of_32_elimination_team, '')
        ) as team_name
    from extracted
),

team_scoped as (
    select
        c.market_id,
        c.question,
        c.category,
        c.description,
        c.outcomes,
        c.market_volume_usd,
        c.is_active,
        c.is_closed,
        c.created_at,
        c.scraped_at,
        c.end_date,
        c.market_slug,
        c.event_slug,
        c.event_id,
        c.condition_id,
        c.sports_market_type,
        c.game_start_time,
        c.group_item_title,
        c.tags,
        c.clob_token_ids,
        c.is_resolved,
        c.winning_outcome,
        c.winning_clob_token_id,
        c.stage_key,
        c.stage_rank,
        c.market_direction,
        c.price_represents,
        c.progression_outcome_label,
        c.team_name,
        ts.team_name as canonical_team_name,
        ts.tournament_status,
        ts.is_still_alive,
        ts.eliminated_stage_key,
        ts.eliminated_match_date,
        ts.next_match_date,
        ts.next_stage_key,
        ts.matches_played,
        ts.wins,
        ts.draws,
        ts.losses,
        ts.goals_for,
        ts.goals_against,
        ts.latest_completed_match_date,
        ts.latest_completed_stage_key
    from classified as c
    left join {{ ref('international_results_wc2026_team_aliases') }} as a
        on lower(c.team_name) = lower(a.market_team_name)
    inner join {{ ref('international_results_wc2026_team_status') }} as ts
        on lower(coalesce(a.canonical_team_name, c.team_name)) = lower(ts.team_name)
    where c.stage_key is not null
)

select
    market_id,
    question,
    category,
    description,
    outcomes,
    market_volume_usd,
    is_active,
    is_closed,
    created_at,
    scraped_at,
    end_date,
    market_slug,
    event_slug,
    event_id,
    condition_id,
    sports_market_type,
    game_start_time,
    group_item_title,
    tags,
    clob_token_ids,
    is_resolved,
    winning_outcome,
    winning_clob_token_id,
    stage_key,
    stage_rank,
    market_direction,
    price_represents,
    progression_outcome_label,
    team_name,
    canonical_team_name,
    tournament_status,
    is_still_alive,
    eliminated_stage_key,
    eliminated_match_date,
    next_match_date,
    next_stage_key,
    matches_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    latest_completed_match_date,
    latest_completed_stage_key,
    case
        when coalesce(is_resolved, false) then 'resolved'
        when coalesce(is_closed, false) then 'closed'
        when coalesce(is_active, false) then 'live'
        else 'inactive'
    end as market_status,
    not coalesce(is_resolved, false)
    and not coalesce(is_closed, false)
    and coalesce(is_active, false) as is_live_market,
    coalesce(is_active, false) and coalesce(is_closed, false) as source_state_anomaly
from team_scoped
