with complete_runs as (
    select * from {{ ref('stg_polymarket_catalog_crawl_runs') }}
    where status = 'complete'
),

latest_crawl as (
    select crawl_id from complete_runs
    qualify row_number() over (order by completed_at desc, crawl_id desc) = 1
),

event_history as (
    select events.* from {{ ref('stg_polymarket_catalog_events') }} as events
    inner join complete_runs on events.crawl_id = complete_runs.crawl_id
),

all_market_history as (
    select markets.* from {{ ref('stg_polymarket_catalog_market_snapshots') }} as markets
    inner join complete_runs on markets.crawl_id = complete_runs.crawl_id
),

qualifying_market_ids as (
    select distinct market_id from all_market_history
    where is_tradable
),

market_history as (
    select markets.* from all_market_history as markets
    inner join qualifying_market_ids on markets.market_id = qualifying_market_ids.market_id
),

edge_history as (
    select edges.* from {{ ref('stg_polymarket_catalog_event_markets') }} as edges
    inner join complete_runs on edges.crawl_id = complete_runs.crawl_id
    inner join qualifying_market_ids on edges.market_id = qualifying_market_ids.market_id
),

latest_events as (
    select * from event_history
    qualify row_number() over (
        partition by event_id order by observed_at desc, crawl_id desc
    ) = 1
),

latest_markets as (
    select * from market_history
    qualify row_number() over (
        partition by market_id order by observed_at desc, crawl_id desc
    ) = 1
),

market_evidence as (
    select
        market_id,
        tradability_evidence_json
    from all_market_history
    where is_tradable
    qualify row_number() over (
        partition by market_id order by observed_at desc, crawl_id desc
    ) = 1
),

latest_edges as (
    select * from edge_history
    qualify row_number() over (
        partition by event_id, market_id order by observed_at desc, crawl_id desc
    ) = 1
),

event_stats as (
    select
        event_id,
        min(observed_at) as first_observed_at,
        max(observed_at) as last_observed_at
    from event_history
    group by event_id
),

market_stats as (
    select
        market_id,
        min(observed_at) as first_observed_at,
        max(observed_at) as last_observed_at
    from market_history
    group by market_id
),

edge_stats as (
    select
        event_id,
        market_id,
        min(observed_at) as first_observed_at,
        max(observed_at) as last_observed_at
    from edge_history
    group by event_id, market_id
),

included_events as (
    select distinct event_id from latest_edges
),

event_nodes as (
    select
        'oddsfox.polymarket.graph-catalog.v1'::varchar as contract_version,
        'event'::varchar as record_type,
        'event:' || events.event_id as record_id,
        events.event_id as entity_id,
        null::varchar as from_record_id,
        null::varchar as to_record_id,
        null::varchar as relationship_type,
        events.title,
        events.subtitle,
        events.description,
        events.resolution_source,
        events.slug,
        case
            when events.slug is not null
                then 'https://polymarket.com/event/' || events.slug
        end as canonical_url,
        events.content_text,
        events.tags_json,
        events.series_json,
        '[]'::varchar as outcomes_json,
        '[]'::varchar as tradability_evidence_json,
        events.attributes_json,
        events.is_active,
        events.is_closed,
        events.is_archived,
        events.is_resolved,
        null::boolean as is_tradable,
        events.source_created_at,
        events.source_updated_at,
        events.start_at,
        events.end_at,
        events.closed_at,
        stats.first_observed_at,
        stats.last_observed_at,
        events.crawl_id as last_observed_crawl_id,
        latest.crawl_id as latest_catalog_crawl_id,
        events.crawl_id = latest.crawl_id as present_in_latest_crawl,
        events.content_text_sha256
    from latest_events as events
    inner join included_events on events.event_id = included_events.event_id
    inner join event_stats as stats on events.event_id = stats.event_id
    cross join latest_crawl as latest
),

market_nodes as (
    select
        'oddsfox.polymarket.graph-catalog.v1'::varchar as contract_version,
        'market'::varchar as record_type,
        'market:' || markets.market_id as record_id,
        markets.market_id as entity_id,
        null::varchar as from_record_id,
        null::varchar as to_record_id,
        null::varchar as relationship_type,
        markets.title,
        markets.subtitle,
        markets.description,
        markets.resolution_source,
        markets.slug,
        case
            when markets.slug is not null
                then 'https://polymarket.com/event/' || markets.slug
        end as canonical_url,
        markets.content_text,
        markets.tags_json,
        '[]'::varchar as series_json,
        markets.outcomes_json,
        evidence.tradability_evidence_json,
        markets.attributes_json,
        markets.is_active,
        markets.is_closed,
        markets.is_archived,
        markets.is_resolved,
        true::boolean as is_tradable,
        markets.source_created_at,
        markets.source_updated_at,
        markets.start_at,
        markets.end_at,
        markets.closed_at,
        stats.first_observed_at,
        stats.last_observed_at,
        markets.crawl_id as last_observed_crawl_id,
        latest.crawl_id as latest_catalog_crawl_id,
        markets.crawl_id = latest.crawl_id as present_in_latest_crawl,
        markets.content_text_sha256
    from latest_markets as markets
    inner join market_evidence as evidence on markets.market_id = evidence.market_id
    inner join market_stats as stats on markets.market_id = stats.market_id
    cross join latest_crawl as latest
),

edge_nodes as (
    select
        'oddsfox.polymarket.graph-catalog.v1'::varchar as contract_version,
        'event_market'::varchar as record_type,
        'event_market:' || edges.event_id || ':' || edges.market_id as record_id,
        null::varchar as entity_id,
        'event:' || edges.event_id as from_record_id,
        'market:' || edges.market_id as to_record_id,
        'contains_market'::varchar as relationship_type,
        null::varchar as title,
        null::varchar as subtitle,
        null::varchar as description,
        null::varchar as resolution_source,
        null::varchar as slug,
        null::varchar as canonical_url,
        edges.content_text,
        '[]'::varchar as tags_json,
        '[]'::varchar as series_json,
        '[]'::varchar as outcomes_json,
        '[]'::varchar as tradability_evidence_json,
        edges.evidence_json as attributes_json,
        null::boolean as is_active,
        null::boolean as is_closed,
        null::boolean as is_archived,
        null::boolean as is_resolved,
        null::boolean as is_tradable,
        null::timestamp as source_created_at,
        null::timestamp as source_updated_at,
        null::timestamp as start_at,
        null::timestamp as end_at,
        null::timestamp as closed_at,
        stats.first_observed_at,
        stats.last_observed_at,
        edges.crawl_id as last_observed_crawl_id,
        latest.crawl_id as latest_catalog_crawl_id,
        edges.crawl_id = latest.crawl_id as present_in_latest_crawl,
        edges.content_text_sha256
    from latest_edges as edges
    inner join latest_events on edges.event_id = latest_events.event_id
    inner join latest_markets on edges.market_id = latest_markets.market_id
    inner join edge_stats as stats
        on edges.event_id = stats.event_id and edges.market_id = stats.market_id
    cross join latest_crawl as latest
)

select * from event_nodes
union all by name
select * from market_nodes
union all by name
select * from edge_nodes
