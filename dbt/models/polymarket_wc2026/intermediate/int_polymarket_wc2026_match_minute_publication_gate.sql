{{ config(materialized='view', tags=['cross_domain']) }}

select
    case
        when blocking_issue_keys is null then true
        else error('WC2026 match-minute publication blocked: ' || blocking_issue_keys)
    end as publication_ready
from {{ ref('polymarket_wc2026_match_minute_odds_data_quality') }}
