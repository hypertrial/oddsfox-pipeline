with catalog as (
    select 100000.0 as min_volume_usd
)

select  -- noqa: ST06
    markets.event_id,
    markets.event_slug,
    markets.market_id,
    markets.question,
    markets.description,
    markets.outcomes,
    markets.clob_token_ids,
    coalesce(markets.game_start_time, markets.event_start_time) as start_time,
    markets.end_date as end_time,
    markets.category,
    markets.tags
from {{ ref('stg_polymarket_catalog_markets') }} as markets
-- costguard: allow cross-join, catalog floor CTE has one row.
cross join catalog
where coalesce(markets.volume, 0) >= catalog.min_volume_usd
