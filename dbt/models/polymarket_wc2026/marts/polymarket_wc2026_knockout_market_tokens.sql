with extracted as (
    select
        t.market_id,
        t.outcome_index,
        t.clob_token_id,
        t.token_updated_at,
        t.question,
        t.outcome_label,
        t.event_slug,
        t.market_slug,
        t.condition_id,
        t.sports_market_type,
        t.game_start_time,
        t.group_item_title,
        t.tags,
        t.clob_token_ids,
        t.is_active,
        t.is_closed,
        t.is_resolved,
        t.winning_outcome,
        t.winning_clob_token_id,
        t.market_volume_usd,
        case
            when t.question like 'Will % win the 2026 FIFA World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' win the 2026 FIFA World Cup?', '')
            else ''
        end as winner_team,
        case
            when t.question like 'Will % reach the 2026 FIFA World Cup final?'
                then replace(replace(t.question, 'Will ', ''), ' reach the 2026 FIFA World Cup final?', '')
            else ''
        end as final_team,
        case
            when t.question like 'Will % reach the Semifinals at the 2026 FIFA World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' reach the Semifinals at the 2026 FIFA World Cup?', '')
            else ''
        end as semifinal_team,
        case
            when t.question like 'Will % reach the Quarterfinals at the 2026 FIFA World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' reach the Quarterfinals at the 2026 FIFA World Cup?', '')
            else ''
        end as quarterfinal_team,
        case
            when t.question like 'Will % reach the Round of 16 at the 2026 FIFA World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' reach the Round of 16 at the 2026 FIFA World Cup?', '')
            else ''
        end as round_of_16_reach_team,
        case
            when t.question like 'Will % be eliminated in the Round of 16 of the World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' be eliminated in the Round of 16 of the World Cup?', '')
            else ''
        end as round_of_16_elimination_team,
        case
            when t.question like 'Will % reach the Round of 32 at the 2026 FIFA World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' reach the Round of 32 at the 2026 FIFA World Cup?', '')
            else ''
        end as round_of_32_reach_team,
        case
            when t.question like 'Will % be eliminated in the Round of 32 of the World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' be eliminated in the Round of 32 of the World Cup?', '')
            else ''
        end as round_of_32_elimination_team
    from {{ ref('int_polymarket_wc2026_market_tokens') }} as t
),

classified as (
    select
        market_id,
        outcome_index,
        clob_token_id,
        token_updated_at,
        question,
        outcome_label,
        event_slug,
        market_slug,
        condition_id,
        sports_market_type,
        game_start_time,
        group_item_title,
        tags,
        clob_token_ids,
        is_active,
        is_closed,
        is_resolved,
        winning_outcome,
        winning_clob_token_id,
        market_volume_usd,
        case
            when winner_team != ''
                then 'winner'
            when final_team != ''
                then 'final'
            when semifinal_team != ''
                then 'semifinal'
            when quarterfinal_team != ''
                then 'quarterfinal'
            when round_of_16_reach_team != '' or round_of_16_elimination_team != ''
                then 'round_of_16'
            when round_of_32_reach_team != '' or round_of_32_elimination_team != ''
                then 'round_of_32'
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
        c.outcome_index,
        c.clob_token_id,
        c.token_updated_at,
        c.question,
        c.outcome_label,
        c.event_slug,
        c.market_slug,
        c.condition_id,
        c.sports_market_type,
        c.game_start_time,
        c.group_item_title,
        c.tags,
        c.clob_token_ids,
        c.is_active,
        c.is_closed,
        c.is_resolved,
        c.winning_outcome,
        c.winning_clob_token_id,
        c.market_volume_usd,
        c.stage_key,
        c.stage_rank,
        c.market_direction,
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
)

select
    c.market_id,
    c.outcome_index,
    c.clob_token_id,
    c.token_updated_at,
    c.question,
    c.outcome_label as source_outcome_label,
    c.event_slug,
    c.market_slug,
    c.condition_id,
    c.sports_market_type,
    c.game_start_time,
    c.group_item_title,
    c.tags,
    c.clob_token_ids,
    c.is_active,
    c.is_closed,
    c.is_resolved,
    c.winning_outcome,
    c.winning_clob_token_id,
    c.market_volume_usd,
    c.stage_key,
    c.stage_rank,
    c.market_direction,
    c.team_name,
    c.canonical_team_name,
    c.tournament_status,
    c.is_still_alive,
    c.eliminated_stage_key,
    c.eliminated_match_date,
    c.next_match_date,
    c.next_stage_key,
    c.matches_played,
    c.wins,
    c.draws,
    c.losses,
    c.goals_for,
    c.goals_against,
    c.latest_completed_match_date,
    c.latest_completed_stage_key,
    case
        when coalesce(c.is_resolved, false) then 'resolved'
        when coalesce(c.is_closed, false) then 'closed'
        when coalesce(c.is_active, false) then 'live'
        else 'inactive'
    end as market_status,
    not coalesce(c.is_resolved, false)
    and not coalesce(c.is_closed, false)
    and coalesce(c.is_active, false) as is_live_market,
    coalesce(c.is_active, false) and coalesce(c.is_closed, false) as source_state_anomaly
from team_scoped as c
where
    c.stage_key is not null
    and (
        (
            c.market_direction in ('winner', 'advance')
            and lower(c.outcome_label) = 'yes'
        )
        or (
            c.market_direction = 'elimination'
            and lower(c.outcome_label) = 'no'
        )
    )
