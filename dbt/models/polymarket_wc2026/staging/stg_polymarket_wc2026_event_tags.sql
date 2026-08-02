{{ config(tags=['wc2026_logical_atlas']) }}

select
    cast(event_id as varchar) as event_id,
    cast(tag_key as varchar) as tag_key,
    cast(observed_at as timestamp) as observed_at,
    nullif(trim(cast(tag_id as varchar)), '') as tag_id,
    lower(nullif(trim(cast(tag_slug as varchar)), '')) as tag_slug,
    nullif(trim(cast(tag_label as varchar)), '') as tag_label
from {{ source('polymarket_wc2026_raw', 'event_tag_snapshots') }}
