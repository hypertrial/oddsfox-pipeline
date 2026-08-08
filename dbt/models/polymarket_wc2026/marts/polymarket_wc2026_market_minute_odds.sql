{{ config(materialized='view', tags=['minute_odds']) }}

-- View over the narrow primary-token fact plus unique market/token dimensions.
-- Avoids rewriting a wide denormalized table after each fact rebuild.
select
    odds.market_id,
    odds.clob_token_id,
    tokens.outcome_label as primary_outcome_label,
    markets.event_id,
    markets.event_slug,
    markets.question,
    markets.market_slug,
    markets.description,
    markets.outcomes,
    markets.condition_id,
    markets.sports_market_type,
    markets.group_item_title,
    markets.tags,
    markets.category,
    markets.is_active,
    markets.is_closed,
    markets.is_resolved,
    markets.winning_outcome,
    markets.winning_clob_token_id,
    markets.market_volume_usd,
    markets.game_start_time,
    markets.end_time,
    markets.created_at,
    markets.event_title,
    markets.event_description,
    markets.event_start_at,
    markets.event_finished_at,
    markets.event_volume_usd_lifetime_reported,
    markets.volume_24h_usd,
    markets.volume_1w_usd,
    markets.volume_1m_usd,
    markets.volume_1y_usd,
    markets.event_liquidity_usd,
    markets.event_is_active,
    markets.event_is_closed,
    markets.event_tags,
    odds.odds_minute_utc,
    odds.odds_minute_epoch,
    odds.open_price as open_odds,
    odds.high_price as high_odds,
    odds.low_price as low_odds,
    odds.close_price as close_odds,
    odds.avg_price as avg_odds,
    odds.observed_points,
    odds.first_observed_at,
    odds.last_observed_at,
    odds.minute_source
from {{ ref('int_polymarket_wc2026_token_minute_odds') }} as odds
inner join {{ ref('int_polymarket_wc2026_primary_market_token') }} as tokens
    on
        odds.market_id = tokens.market_id
        and odds.clob_token_id = tokens.clob_token_id
inner join {{ ref('int_polymarket_wc2026_markets') }} as markets
    on odds.market_id = markets.market_id
