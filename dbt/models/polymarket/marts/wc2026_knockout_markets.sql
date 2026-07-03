with current_token_prices as (
    select
        clob_token_id,
        close_price as current_price,
        odds_hour_utc as current_price_hour_utc,
        odds_hour_epoch as current_price_hour_epoch
    from {{ ref('selected_token_live_hourly_odds') }}
    qualify row_number() over (
        partition by clob_token_id
        order by odds_hour_epoch desc
    ) = 1
),

classified as (
    select
        t.market_id,
        t.outcome_index,
        t.clob_token_id,
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
        p.current_price,
        p.current_price_hour_utc,
        p.current_price_hour_epoch,
        case
            when regexp_extract(t.question, '^Will (.*) win the 2026 FIFA World Cup\\?$', 1) != ''
                then 'winner'
            when regexp_extract(t.question, '^Will (.*) reach the 2026 FIFA World Cup final\\?$', 1) != ''
                then 'final'
            when regexp_extract(t.question, '^Will (.*) reach the Semifinals at the 2026 FIFA World Cup\\?$', 1) != ''
                then 'semifinal'
            when regexp_extract(t.question, '^Will (.*) reach the Quarterfinals at the 2026 FIFA World Cup\\?$', 1) != ''
                then 'quarterfinal'
            when regexp_extract(t.question, '^Will (.*) reach the Round of 16 at the 2026 FIFA World Cup\\?$', 1) != ''
                then 'round_of_16'
            when regexp_extract(t.question, '^Will (.*) reach the Round of 32 at the 2026 FIFA World Cup\\?$', 1) != ''
                then 'round_of_32'
        end as stage_key,
        case
            when regexp_extract(t.question, '^Will (.*) win the 2026 FIFA World Cup\\?$', 1) != '' then 5
            when regexp_extract(t.question, '^Will (.*) reach the 2026 FIFA World Cup final\\?$', 1) != '' then 4
            when regexp_extract(t.question, '^Will (.*) reach the Semifinals at the 2026 FIFA World Cup\\?$', 1) != '' then 3
            when regexp_extract(t.question, '^Will (.*) reach the Quarterfinals at the 2026 FIFA World Cup\\?$', 1) != '' then 2
            when regexp_extract(t.question, '^Will (.*) reach the Round of 16 at the 2026 FIFA World Cup\\?$', 1) != '' then 1
            when regexp_extract(t.question, '^Will (.*) reach the Round of 32 at the 2026 FIFA World Cup\\?$', 1) != '' then 0
        end as stage_rank,
        coalesce(
            nullif(regexp_extract(t.question, '^Will (.*) win the 2026 FIFA World Cup\\?$', 1), ''),
            nullif(regexp_extract(t.question, '^Will (.*) reach the 2026 FIFA World Cup final\\?$', 1), ''),
            nullif(regexp_extract(t.question, '^Will (.*) reach the Semifinals at the 2026 FIFA World Cup\\?$', 1), ''),
            nullif(regexp_extract(t.question, '^Will (.*) reach the Quarterfinals at the 2026 FIFA World Cup\\?$', 1), ''),
            nullif(regexp_extract(t.question, '^Will (.*) reach the Round of 16 at the 2026 FIFA World Cup\\?$', 1), ''),
            nullif(regexp_extract(t.question, '^Will (.*) reach the Round of 32 at the 2026 FIFA World Cup\\?$', 1), '')
        ) as team_name
    from {{ ref('int_polymarket_selected_token_universe') }} as t
    left join current_token_prices as p
        on t.clob_token_id = p.clob_token_id
)

select
    market_id,
    outcome_index,
    clob_token_id,
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
    current_price,
    current_price_hour_utc,
    current_price_hour_epoch,
    stage_key,
    stage_rank,
    team_name
from classified
where stage_key is not null
