{{ config(tags=['wc2026_logical_atlas']) }}

select
    cast(event_id as varchar) as event_id,
    cast(market_id as varchar) as market_id,
    cast(source_ordinal as bigint) as source_ordinal,
    cast(is_enclosing_event as boolean) as is_enclosing_event,
    cast(observed_at as timestamp) as observed_at
from {{ source('polymarket_wc2026_raw', 'event_market_snapshots') }}
