{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_market_hourly_odds'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'market_hourly_odds']
        }
    }
) }}

-- Every admitted primary token with hourly history must appear in the golden mart.
select p.market_id
from {{ ref('int_polymarket_wc2026_primary_market_token') }} as p
inner join {{ ref('int_polymarket_wc2026_token_hourly_odds') }} as h
    on p.clob_token_id = h.clob_token_id
left join {{ ref('polymarket_wc2026_market_hourly_odds') }} as m
    on p.market_id = m.market_id
where m.market_id is null
group by p.market_id
