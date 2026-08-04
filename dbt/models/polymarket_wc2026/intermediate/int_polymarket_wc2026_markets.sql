with registry as (
    select
        market_id,
        event_id,
        event_slug,
        scope_name,
        event_volume_usd_lifetime_reported,
        is_event_volume_eligible,
        first_eligible_at
    from {{ source('polymarket_wc2026_ops', 'market_scope_registry') }}
    where
        lower(scope_name) = 'wc2026'
        and coalesce(is_event_volume_eligible, false)
),

enclosing_events as (
    select
        market_id,
        event_id
    from (
        select
            market_id,
            event_id,
            is_enclosing_event,
            row_number() over (
                partition by market_id
                order by observed_at desc
            ) as rn
        from {{ ref('stg_polymarket_wc2026_event_market_snapshots') }}
    )
    where
        rn = 1
        and is_enclosing_event
)

select
    markets.market_id,
    markets.question,
    markets.category,
    markets.description,
    markets.outcomes,
    markets.volume as market_volume_usd,
    markets.is_active,
    markets.is_closed,
    markets.created_at,
    markets.scraped_at,
    markets.end_date as end_time,
    markets.slug as market_slug,
    markets.condition_id,
    markets.sports_market_type,
    markets.game_start_time,
    markets.group_item_title,
    markets.tags,
    markets.clob_token_ids,
    markets.is_resolved,
    markets.winning_outcome,
    markets.winning_clob_token_id,
    registry.scope_name,
    registry.event_volume_usd_lifetime_reported,
    registry.is_event_volume_eligible,
    registry.first_eligible_at,
    events.event_title,
    events.event_description,
    events.event_start_at,
    events.finished_at as event_finished_at,
    events.volume_24h_usd,
    events.volume_1w_usd,
    events.volume_1m_usd,
    events.volume_1y_usd,
    events.liquidity_usd as event_liquidity_usd,
    events.is_active as event_is_active,
    events.is_closed as event_is_closed,
    events.tags_json as event_tags,
    coalesce(registry.event_slug, markets.event_slug) as event_slug,
    coalesce(registry.event_id, enclosing_events.event_id, markets.event_id) as event_id
from {{ ref('stg_polymarket_wc2026_markets') }} as markets
inner join registry
    on markets.market_id = registry.market_id
left join enclosing_events
    on markets.market_id = enclosing_events.market_id
left join {{ ref('int_polymarket_wc2026_event_latest') }} as events
    on coalesce(registry.event_id, enclosing_events.event_id, markets.event_id) = events.event_id
