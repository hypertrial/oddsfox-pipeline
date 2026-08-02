{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_logical_events'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'logical_events']
        }
    }
) }}

select event_id
from {{ ref('polymarket_wc2026_logical_events') }}
where
    event_logical_eligible
    and (
        not ever_eligible
        or first_eligible_observed_at is null
        or event_created_at is null
        or eligibility_effective_from is null
        or eligibility_effective_from is distinct from event_created_at
        or membership_status != 'included'
    )
