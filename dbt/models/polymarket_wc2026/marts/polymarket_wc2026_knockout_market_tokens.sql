select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.token_updated_at,
    c.question,
    t.outcome_label as source_outcome_label,
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
    c.price_represents,
    c.progression_outcome_label,
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
    c.market_status,
    c.is_live_market,
    c.source_state_anomaly,
    coalesce(c.is_live_market and c.is_still_alive, false) as is_active_team_live_market
from {{ ref('int_polymarket_wc2026_market_tokens') }} as t
inner join {{ ref('int_polymarket_wc2026_knockout_market_classification') }} as c
    on t.market_id = c.market_id
where
    (
        c.market_direction in ('winner', 'advance')
        and lower(t.outcome_label) = 'yes'
    )
    or (
        c.market_direction = 'elimination'
        and lower(t.outcome_label) = 'no'
    )
