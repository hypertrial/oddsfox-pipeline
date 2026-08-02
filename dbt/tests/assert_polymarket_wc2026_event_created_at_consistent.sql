{{ config(tags=['wc2026_logical_atlas']) }}

select event_id
from {{ ref('stg_polymarket_wc2026_event_snapshots') }}
group by event_id
having count(distinct created_at) > 1
