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
