with tied_knockout_advancer_unknown as (
    select
        'warn' as severity,
        'match' as entity_type,
        match_id,
        cast(null as varchar) as team_name,
        stage_key,
        'Completed tied knockout match has no unique later-fixture advancer inference.'
            as issue_detail,
        'knockout_tied_advancer_unknown:' || match_id as issue_key,
        current_timestamp as observed_at
    from {{ ref('international_results_wc2026_matches') }}
    where
        is_knockout
        and match_status = 'completed'
        and is_tied
        and advancer_inference_status in (
            'missing_later_fixture',
            'ambiguous_later_fixture'
        )
),

source_freshness as (
    select max(source_loaded_at) as latest_source_loaded_at
    from {{ ref('international_results_wc2026_matches') }}
),

stale_source as (
    select
        'warn' as severity,
        'source' as entity_type,
        cast(null as varchar) as match_id,
        cast(null as varchar) as team_name,
        cast(null as varchar) as stage_key,
        'WC2026 international_results source latest load is older than 12 hours.'
            as issue_detail,
        'international_results_source_stale' as issue_key,
        current_timestamp as observed_at
    from source_freshness
    where
        latest_source_loaded_at is null
        or latest_source_loaded_at < cast(current_timestamp as timestamp) - interval 12 hour
)

select *
from tied_knockout_advancer_unknown
union all
select *
from stale_source
