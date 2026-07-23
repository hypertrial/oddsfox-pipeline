{{ config(materialized='view', tags=['polygon_settlement']) }}

select
    case
        when publication_ready then true
        else error(
            'WC2026 Polygon settlement publication blocked: '
            || blocking_issue_keys
        )
    end as publication_ready
from {{ ref('polymarket_wc2026_polygon_settlement_data_quality') }}
