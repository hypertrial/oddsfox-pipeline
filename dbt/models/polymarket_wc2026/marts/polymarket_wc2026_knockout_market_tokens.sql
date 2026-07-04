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
            when t.question like 'Will % be eliminated in the Round of 16 of the World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' be eliminated in the Round of 16 of the World Cup?', '')
            else ''
        end as round_of_16_team,
        case
            when t.question like 'Will % reach the Round of 32 at the 2026 FIFA World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' reach the Round of 32 at the 2026 FIFA World Cup?', '')
            when t.question like 'Will % be eliminated in the Round of 32 of the World Cup?'
                then replace(replace(t.question, 'Will ', ''), ' be eliminated in the Round of 32 of the World Cup?', '')
            else ''
        end as round_of_32_team
    from {{ ref('polymarket_wc2026_market_tokens') }} as t
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
            when round_of_16_team != ''
                then 'round_of_16'
            when round_of_32_team != ''
                then 'round_of_32'
        end as stage_key,
        case
            when winner_team != '' then 5
            when final_team != '' then 4
            when semifinal_team != '' then 3
            when quarterfinal_team != '' then 2
            when round_of_16_team != '' then 1
            when round_of_32_team != '' then 0
        end as stage_rank,
        coalesce(
            nullif(winner_team, ''),
            nullif(final_team, ''),
            nullif(semifinal_team, ''),
            nullif(quarterfinal_team, ''),
            nullif(round_of_16_team, ''),
            nullif(round_of_32_team, '')
        ) as team_name
    from extracted
),

sibling_tokens as (
    select
        market_id,
        max(case when lower(outcome_label) = 'yes' then clob_token_id end) as yes_clob_token_id,
        max(case when lower(outcome_label) = 'no' then clob_token_id end) as no_clob_token_id
    from classified
    group by 1
)

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
    s.yes_clob_token_id,
    s.no_clob_token_id,
    c.is_active,
    c.is_closed,
    c.is_resolved,
    c.winning_outcome,
    c.winning_clob_token_id,
    c.market_volume_usd,
    c.stage_key,
    c.stage_rank,
    c.team_name,
    case
        when c.clob_token_id = s.yes_clob_token_id then s.no_clob_token_id
        when c.clob_token_id = s.no_clob_token_id then s.yes_clob_token_id
    end as opposite_clob_token_id
from classified as c
left join sibling_tokens as s
    on c.market_id = s.market_id
where c.stage_key is not null
