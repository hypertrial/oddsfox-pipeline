{{ config(severity = 'warn') }}

select
    s.market_id,
    s.clob_token_id,
    s.stage_key,
    s.team_name,
    s.market_status
from {{ ref('polymarket_wc2026_knockout_markets') }} as s
left join {{ ref('polymarket_wc2026_knockout_data_quality') }} as d
    on
        d.issue_key = 'source_state_anomaly:'
        || s.stage_key
        || ':'
        || s.market_direction
        || ':'
        || s.market_status
        and d.severity = 'warn'
where
    s.source_state_anomaly
    and d.issue_key is null
