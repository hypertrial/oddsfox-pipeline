{{ config(tags=['polygon_settlement']) }}

select blocking_issue_keys
from {{ ref('polymarket_wc2026_polygon_settlement_data_quality') }}
where blocking_issue_keys is not null
