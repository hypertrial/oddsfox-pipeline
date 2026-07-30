with catalog as (
    select 100000.0 as min_volume_usd
),

admitted as (
    select
        markets.market_id,
        markets.description,
        markets.outcomes,
        markets.clob_token_ids,
        markets.volume,
        markets.end_date as end_time,
        markets.tags,
        nullif(trim(markets.event_id), '') as event_id,
        nullif(trim(markets.event_slug), '') as event_slug,
        nullif(trim(markets.question), '') as question,
        coalesce(markets.game_start_time, markets.event_start_time) as raw_start_time,
        nullif(trim(markets.category), '') as category
    from {{ ref('stg_polymarket_catalog_markets') }} as markets
    -- costguard: allow cross-join, catalog floor CTE has one row.
    cross join catalog
    where coalesce(markets.volume, 0) >= catalog.min_volume_usd
)

select  -- noqa: ST06
    event_id,
    event_slug,
    market_id,
    question,
    description,
    outcomes,
    clob_token_ids,
    volume,
    case
        when end_time is not null and raw_start_time > end_time then null
        else raw_start_time
    end as start_time,
    end_time,
    category,
    tags
from admitted
