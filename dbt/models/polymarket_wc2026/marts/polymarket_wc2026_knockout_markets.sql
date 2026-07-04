with current_token_prices as (
    select
        clob_token_id,
        arg_max(close_price, odds_hour_epoch) as current_price,
        arg_max(odds_hour_utc, odds_hour_epoch) as current_price_hour_utc,
        max(odds_hour_epoch) as current_price_hour_epoch
    from {{ ref('polymarket_wc2026_knockout_token_hourly_odds') }}
    group by 1
),

with_prices as (
    select
        k.market_id,
        k.outcome_index,
        k.clob_token_id,
        k.question,
        k.source_outcome_label,
        k.event_slug,
        k.market_slug,
        k.condition_id,
        k.sports_market_type,
        k.game_start_time,
        k.group_item_title,
        k.tags,
        k.clob_token_ids,
        k.is_active,
        k.is_closed,
        k.is_resolved,
        k.market_status,
        k.is_live_market,
        k.source_state_anomaly,
        k.winning_outcome,
        k.winning_clob_token_id,
        k.market_volume_usd,
        p.current_price,
        p.current_price_hour_utc,
        p.current_price_hour_epoch,
        k.stage_key,
        k.stage_rank,
        k.market_direction,
        k.price_represents,
        k.progression_outcome_label,
        k.team_name,
        k.canonical_team_name,
        k.tournament_status,
        k.is_still_alive,
        k.eliminated_stage_key,
        k.eliminated_match_date,
        k.next_match_date,
        k.next_stage_key,
        k.matches_played,
        k.wins,
        k.draws,
        k.losses,
        k.goals_for,
        k.goals_against,
        k.latest_completed_match_date,
        k.latest_completed_stage_key,
        case
            when p.current_price_hour_epoch is not null
                then round((epoch(current_timestamp) - p.current_price_hour_epoch) / 3600.0, 4)
        end as current_price_age_hours
    from {{ ref('polymarket_wc2026_knockout_market_tokens') }} as k
    left join current_token_prices as p
        on k.clob_token_id = p.clob_token_id
)

select
    market_id,
    outcome_index,
    clob_token_id,
    question,
    source_outcome_label,
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
    market_status,
    is_live_market,
    source_state_anomaly,
    winning_outcome,
    winning_clob_token_id,
    market_volume_usd,
    current_price,
    current_price_hour_utc,
    current_price_hour_epoch,
    current_price_age_hours,
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
        when market_status = 'resolved' then 'historical_resolved'
        when market_status = 'closed' then 'historical_closed'
        when market_status = 'inactive' then 'inactive'
        when market_status = 'live' and current_price is null then 'missing_live'
        when market_status = 'live' and current_price_age_hours <= 3 then 'fresh_live'
        when market_status = 'live' then 'stale_live'
    end as current_price_status,
    coalesce(market_status = 'live' and current_price_age_hours <= 3, false) as is_current_price_fresh
from with_prices
