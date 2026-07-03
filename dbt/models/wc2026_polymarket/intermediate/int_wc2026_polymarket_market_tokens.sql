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
    t.market_volume_usd
from {{ ref('int_wc2026_polymarket_token_universe') }} as t
where t.market_id in (
    select wc2026_markets.market_id
    from {{ ref('int_wc2026_polymarket_markets') }} as wc2026_markets
)
