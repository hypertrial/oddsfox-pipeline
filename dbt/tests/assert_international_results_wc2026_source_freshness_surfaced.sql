{{ config(severity = 'warn') }}

with source_freshness as (
    select max(source_loaded_at) as latest_source_loaded_at
    from {{ ref('international_results_wc2026_matches') }}
),

stale_source as (
    select latest_source_loaded_at
    from source_freshness
    where
        latest_source_loaded_at is null
        or latest_source_loaded_at < cast(current_timestamp as timestamp) - interval 12 hour
)

select s.latest_source_loaded_at
from stale_source as s
left join {{ ref('international_results_wc2026_data_quality') }} as d
    on
        d.issue_key = 'international_results_source_stale'
        and d.severity = 'warn'
        and d.entity_type = 'source'
where d.issue_key is null
