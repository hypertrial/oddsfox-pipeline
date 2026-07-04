with markets as (
    select
        market_id,
        question,
        category,
        description,
        outcomes,
        volume,
        is_active,
        is_closed,
        created_at,
        scraped_at,
        end_date,
        slug,
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
        winning_clob_token_id
    from {{ ref('stg_polymarket_wc2026_markets') }}
),

registry as (
    select
        market_id,
        scope_name
    from {{ source('polymarket_wc2026_ops', 'market_scope_registry') }}
    where lower(scope_name) = 'wc2026'
)

select
    markets.market_id,
    markets.question,
    markets.category,
    markets.description,
    markets.outcomes,
    markets.volume,
    markets.is_active,
    markets.is_closed,
    markets.created_at,
    markets.scraped_at,
    markets.end_date,
    markets.slug,
    markets.event_slug,
    markets.event_id,
    markets.condition_id,
    markets.sports_market_type,
    markets.game_start_time,
    markets.group_item_title,
    markets.tags,
    markets.clob_token_ids,
    markets.is_resolved,
    markets.winning_outcome,
    markets.winning_clob_token_id,
    registry.scope_name
from markets
inner join registry
    on markets.market_id = registry.market_id
-- ponytail: keep in sync with POLYMARKET_WC2026_WHALE_MIN_VOLUME_USD (settings_polymarket.py).
where coalesce(markets.volume, 0) >= 100000
