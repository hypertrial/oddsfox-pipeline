{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_knockout_token_hourly_odds'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'knockout_token_hourly_odds']
        }
    }
) }}

select
    odds.clob_token_id,
    odds.odds_hour_utc
from {{ ref('polymarket_wc2026_knockout_token_hourly_odds') }} as odds
-- costguard: allow cross-join, WC2026 contract seed has one row.
cross join {{ ref('polymarket_wc2026_contract') }} as contract
where
    contract.scope_name = 'wc2026'
    and odds.odds_hour_utc < current_timestamp - (contract.hourly_window_days * interval '1 day')
