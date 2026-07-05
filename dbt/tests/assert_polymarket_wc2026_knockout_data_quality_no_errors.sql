{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_knockout_data_quality'},
            'asset_key': ['polymarket', 'wc2026', 'observability', 'knockout_data_quality']
        }
    }
) }}

select *
from {{ ref('polymarket_wc2026_knockout_data_quality') }}
where severity = 'error'
