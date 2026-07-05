{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_knockout_stage_coverage'},
            'asset_key': ['polymarket', 'wc2026', 'observability', 'knockout_stage_coverage']
        }
    }
) }}

select
    stage_key,
    market_direction,
    market_status,
    expected_hourly_rows,
    hourly_rows,
    hourly_completeness_ratio
from {{ ref('polymarket_wc2026_knockout_stage_coverage') }}
where
    hourly_completeness_ratio < 0
    or hourly_completeness_ratio > 1
