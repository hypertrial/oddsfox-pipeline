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
        d.issue_key in (
            'live_missing_hourly_odds:' || s.market_id || ':' || s.clob_token_id,
            'historical_missing_hourly_odds:' || s.market_id || ':' || s.clob_token_id,
            'inactive_missing_hourly_odds:' || s.market_id || ':' || s.clob_token_id
        )
        and d.severity = 'warn'
where
    s.current_price is null
    and d.issue_key is null
