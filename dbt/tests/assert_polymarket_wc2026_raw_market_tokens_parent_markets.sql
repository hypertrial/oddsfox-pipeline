{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'stg_polymarket_wc2026_market_tokens'},
            'asset_key': ['polymarket', 'wc2026', 'staging', 'market_tokens']
        }
    }
) }}

select t.market_id
from {{ ref('stg_polymarket_wc2026_market_tokens') }} as t
left join {{ ref('stg_polymarket_wc2026_markets') }} as m
    on t.market_id = m.market_id
where
    t.market_id is not null
    and m.market_id is null
