{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'kalshi_wc2026_data_quality'},
            'asset_key': ['kalshi', 'wc2026', 'observability', 'data_quality']
        }
    }
) }}

select *
from {{ ref('kalshi_wc2026_data_quality') }}
where severity = 'error'
