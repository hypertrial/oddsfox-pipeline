{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_knockout_markets'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'knockout_markets']
        }
    }
) }}

select
    market_id,
    clob_token_id,
    team_name,
    market_status,
    is_active_team_live_market,
    current_price_status,
    is_actionable_live_market
from {{ ref('polymarket_wc2026_knockout_markets') }}
where
    is_actionable_live_market != coalesce(
        is_active_team_live_market and current_price_status = 'fresh_live',
        false
    )
