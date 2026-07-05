{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_knockout_data_quality'},
            'asset_key': ['polymarket', 'wc2026', 'observability', 'knockout_data_quality']
        }
    }
) }}

with live_freshness_dq as (
    select
        issue_key,
        market_id,
        clob_token_id,
        team_name,
        market_status
    from {{ ref('polymarket_wc2026_knockout_data_quality') }}
    where
        issue_key like 'live_missing_hourly_odds:%'
        or issue_key like 'live_stale_hourly_odds:%'
)

select
    d.issue_key,
    d.market_id,
    d.clob_token_id,
    d.team_name,
    d.market_status,
    s.is_active_team_live_market,
    s.is_still_alive
from live_freshness_dq as d
left join {{ ref('polymarket_wc2026_knockout_markets') }} as s
    on
        d.market_id = s.market_id
        and d.clob_token_id = s.clob_token_id
where not coalesce(s.is_active_team_live_market, false)
