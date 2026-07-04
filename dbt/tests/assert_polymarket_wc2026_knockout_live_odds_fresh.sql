{{ config(severity = 'warn') }}

select
    market_id,
    clob_token_id,
    stage_key,
    team_name,
    current_price_status,
    current_price_hour_utc,
    current_price_age_hours
from {{ ref('polymarket_wc2026_knockout_markets') }}
where
    market_status = 'live'
    and current_price_status in ('missing_live', 'stale_live')
