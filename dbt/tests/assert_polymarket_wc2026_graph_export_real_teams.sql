{{
    config(
        meta={
            'dagster': {
                'ref': {'name': 'polymarket_wc2026_graph_token_hourly_odds'},
                'asset_key': ['polymarket', 'wc2026', 'marts', 'graph_token_hourly_odds']
            }
        }
    )
}}

select distinct
    market_id,
    question
from {{ ref('polymarket_wc2026_graph_token_hourly_odds') }}
where canonical_team_name is null
