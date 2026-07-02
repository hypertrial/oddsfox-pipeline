{% set active_market_scope = var('active_market_scope', 'wc2026') | lower %}

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
        event_id
    from {{ ref('stg_polymarket_markets') }}
),

registry as (
    select market_id
    from {{ source('polymarket_ops', 'market_scope_registry') }}
    where lower(scope_name) = '{{ active_market_scope }}'
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
    '{{ active_market_scope }}' as active_market_scope,
    {{ 'true' if active_market_scope == 'all' else 'false' }} as is_all_scope
from markets
{% if active_market_scope != 'all' %}
    inner join registry
        on markets.market_id = registry.market_id
{% endif %}
