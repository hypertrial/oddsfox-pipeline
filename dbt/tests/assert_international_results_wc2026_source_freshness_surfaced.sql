{{ config(
    severity = 'warn',
    meta = {
        'dagster': {
            'ref': {'name': 'international_results_wc2026_data_quality'},
            'asset_key': ['international_results', 'wc2026', 'observability', 'data_quality']
        }
    }
) }}

with contract as (
    select results_freshness_hours
    from {{ ref('polymarket_wc2026_contract') }}
    where scope_name = 'wc2026'
),

source_freshness as (
    select max(source_loaded_at) as latest_source_loaded_at
    from {{ ref('international_results_wc2026_matches') }}
),

stale_source as (
    select source_freshness.latest_source_loaded_at
    from source_freshness
    -- costguard: allow cross-join, WC2026 contract seed has one row.
    cross join contract
    where
        source_freshness.latest_source_loaded_at is null
        or source_freshness.latest_source_loaded_at < cast(current_timestamp as timestamp)
        - (contract.results_freshness_hours * interval '1 hour')
)

select s.latest_source_loaded_at
from stale_source as s
left join {{ ref('international_results_wc2026_data_quality') }} as d
    on
        d.issue_key = 'international_results_source_stale'
        and d.severity = 'warn'
        and d.entity_type = 'source'
where d.issue_key is null
