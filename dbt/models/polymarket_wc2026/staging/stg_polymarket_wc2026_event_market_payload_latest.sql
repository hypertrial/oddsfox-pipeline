{{ config(tags=['wc2026_logical_atlas']) }}

select *
from {{ ref('stg_polymarket_wc2026_event_market_payload_snapshots') }}
qualify row_number() over (
    partition by market_id
    order by observed_at desc, scraped_at desc nulls last
) = 1
