{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_logical_market_events'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'logical_market_events']
        }
    }
) }}

select market_id
from {{ ref('polymarket_wc2026_logical_market_events') }}
group by market_id
having count(*) filter (where is_primary_qualifying_event) != 1
