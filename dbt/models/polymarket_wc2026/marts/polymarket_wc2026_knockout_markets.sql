with current_token_prices as (
    select
        clob_token_id,
        arg_max(close_price, odds_hour_epoch) as current_price,
        arg_max(odds_hour_utc, odds_hour_epoch) as current_price_hour_utc,
        max(odds_hour_epoch) as current_price_hour_epoch
    from {{ ref('polymarket_wc2026_knockout_token_hourly_odds') }}
    group by 1
)

select
    k.market_id,
    k.outcome_index,
    k.clob_token_id,
    k.question,
    k.outcome_label,
    k.event_slug,
    k.market_slug,
    k.condition_id,
    k.sports_market_type,
    k.game_start_time,
    k.group_item_title,
    k.tags,
    k.clob_token_ids,
    k.yes_clob_token_id,
    k.no_clob_token_id,
    k.opposite_clob_token_id,
    k.is_active,
    k.is_closed,
    k.is_resolved,
    k.winning_outcome,
    k.winning_clob_token_id,
    k.market_volume_usd,
    p.current_price,
    p.current_price_hour_utc,
    p.current_price_hour_epoch,
    k.stage_key,
    k.stage_rank,
    k.team_name
from {{ ref('polymarket_wc2026_knockout_market_tokens') }} as k
left join current_token_prices as p
    on k.clob_token_id = p.clob_token_id
