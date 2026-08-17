{{ config(materialized='table') }}

select
    event_id,
    event_title,
    exclusion_reason,
    refreshed_at
from {{ source('polymarket_soccer_ops', 'match_result_registry_exclusions') }}
