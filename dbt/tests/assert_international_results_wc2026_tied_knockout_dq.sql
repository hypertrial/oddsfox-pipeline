{{ config(
    tags = ['cross_domain'],
    meta = {
        'dagster': {
            'ref': {'name': 'international_results_wc2026_data_quality'},
            'asset_key': ['international_results', 'wc2026', 'observability', 'data_quality']
        }
    }
) }}

select
    m.match_id,
    m.stage_key
from {{ ref('international_results_wc2026_matches') }} as m
left join {{ ref('international_results_wc2026_data_quality') }} as dq
    on m.match_id = dq.match_id
where
    m.is_knockout
    and m.match_status = 'completed'
    and m.is_tied
    and m.advancing_team is null
    and dq.issue_key is null
