{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_graph_catalog'},
            'asset_key': ['polymarket', 'catalog', 'marts', 'polymarket_graph_catalog']
        }
    }
) }}

with graph as (
    select * from {{ ref('polymarket_graph_catalog') }}
),

complete_runs as (
    select crawl_id from {{ ref('stg_polymarket_catalog_crawl_runs') }}
    where status = 'complete'
),

qualifying_markets as (
    select distinct markets.market_id
    from {{ ref('stg_polymarket_catalog_market_snapshots') }} as markets
    inner join complete_runs on markets.crawl_id = complete_runs.crawl_id
    where markets.is_tradable
),

qualifying_edges as (
    select distinct
        edges.event_id,
        edges.market_id
    from {{ ref('stg_polymarket_catalog_event_markets') }} as edges
    inner join complete_runs on edges.crawl_id = complete_runs.crawl_id
    inner join qualifying_markets on edges.market_id = qualifying_markets.market_id
),

source_counts as (
    select
        (select count(*) from qualifying_markets) as markets,
        (select count(*) from qualifying_edges) as edges,
        (select count(distinct event_id) from qualifying_edges) as events
),

graph_counts as (
    select
        count(*) filter (where record_type = 'market') as markets,
        count(*) filter (where record_type = 'event_market') as edges,
        count(*) filter (where record_type = 'event') as events
    from graph
),

violations as (
    select
        record_id,
        'market_without_tradability' as reason
    from graph
    where record_type = 'market' and not coalesce(is_tradable, false)
    union all
    select
        record_id,
        'event_without_edge' as reason
    from graph as event_node
    where
        event_node.record_type = 'event' and not exists (
            select 1 from graph as edge
            where
                edge.record_type = 'event_market'
                and edge.from_record_id = event_node.record_id
        )
    union all
    select
        edge.record_id,
        'dangling_edge' as reason
    from graph as edge
    where
        edge.record_type = 'event_market' and (
            not exists (
                select 1 from graph as node
                where node.record_id = edge.from_record_id
            )
            or not exists (
                select 1 from graph as node
                where node.record_id = edge.to_record_id
            )
        )
    union all
    select
        record_id,
        'invalid_relationship_type' as reason
    from graph
    where record_type = 'event_market' and relationship_type != 'contains_market'
    union all
    select
        record_id,
        'invalid_graph_identity' as reason
    from graph
    where
        (
            record_type = 'event'
            and (
                entity_id is null
                or record_id != 'event:' || entity_id
                or from_record_id is not null
                or to_record_id is not null
            )
        )
        or (
            record_type = 'market'
            and (
                entity_id is null
                or record_id != 'market:' || entity_id
                or from_record_id is not null
                or to_record_id is not null
            )
        )
        or (
            record_type = 'event_market'
            and (
                entity_id is not null
                or from_record_id is null
                or to_record_id is null
                or record_id != 'event_market:'
                || substring(from_record_id, 7)
                || ':'
                || substring(to_record_id, 8)
            )
        )
    union all
    select
        record_id,
        'invalid_text_or_json' as reason
    from graph
    where
        content_text = ''
        or content_text_sha256 != sha256(content_text)
        or try_cast(tags_json as json) is null
        or try_cast(series_json as json) is null
        or try_cast(outcomes_json as json) is null
        or try_cast(tradability_evidence_json as json) is null
        or try_cast(attributes_json as json) is null
        or (
            record_type = 'market'
            and json_array_length(tradability_evidence_json) = 0
        )
    union all
    select
        'catalog' as record_id,
        'raw_count_mismatch' as reason
    from source_counts
    cross join graph_counts
    where
        source_counts.markets != graph_counts.markets
        or source_counts.edges != graph_counts.edges
        or source_counts.events != graph_counts.events
)

select * from violations
