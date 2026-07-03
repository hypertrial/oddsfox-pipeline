{% set active_market_scopes = var('active_market_scopes', ['wc2026']) %}
{% if active_market_scopes is string %}
    {% set active_market_scopes = [active_market_scopes] %}
{% endif %}
{% set normalized_scopes = active_market_scopes | map('lower') | list %}
{% set is_all_scope = 'all' in normalized_scopes %}

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
    from {{ ref('stg_polymarket_markets') }}
),

registry as (
    select
        market_id,
        scope_name
    from {{ source('polymarket_ops', 'market_scope_registry') }}
    {% if not is_all_scope %}
        where lower(scope_name) in (
            {% for scope in normalized_scopes -%}
                '{{ scope }}'{% if not loop.last %}, {% endif %}
            {%- endfor %}
        )
    {% endif %}
)

{% if is_all_scope %}
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
    'all' as scope_name,
    true as is_all_scope
from markets
{% else %}
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
        registry.scope_name,
        false as is_all_scope
    from markets
    inner join registry
        on markets.market_id = registry.market_id
{% endif %}
