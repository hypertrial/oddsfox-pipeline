{{ config(materialized='table', tags=['polygon_settlement']) }}

with published_scans as (
    select *
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_runs') }}
    where status = 'published' and raw_published
)

select *
from published_scans
order by published_at desc nulls last, finished_at desc nulls last, scan_id desc
limit 1
