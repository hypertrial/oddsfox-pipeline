with catalog as (
    select 100000.0 as min_volume_usd
),

markets as (
    select
        event_id,
        event_slug,
        market_id,
        question,
        description,
        outcomes,
        clob_token_ids,
        game_start_time as start_time,
        end_date as end_time,
        category,
        tags,
        volume
    from {{ ref('stg_polymarket_us_midterms_2026_markets') }}
),

registry as (
    select market_id
    from {{ source('polymarket_us_midterms_2026_ops', 'market_scope_registry') }}
    where lower(scope_name) = 'us_midterms_2026'
)

select
    markets.event_id,
    markets.event_slug,
    markets.market_id,
    markets.question,
    markets.description,
    markets.outcomes,
    markets.clob_token_ids,
    markets.start_time,
    markets.end_time,
    markets.category,
    markets.tags
from markets
inner join registry
    on markets.market_id = registry.market_id
-- costguard: allow cross-join, catalog floor CTE has one row.
cross join catalog
where coalesce(markets.volume, 0) >= catalog.min_volume_usd
