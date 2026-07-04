{{ config(severity = 'warn') }}

select
    issue_key,
    issue_count
from {{ ref('polymarket_wc2026_knockout_data_quality') }}
where
    issue_key like 'source_state_anomaly:%'
    and (
        issue_count is null
        or issue_count <= 0
        or entity_type != 'stage'
    )
