{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'int_polymarket_wc2026_event_membership'},
            'asset_key': ['polymarket', 'wc2026', 'intermediate', 'event_membership']
        }
    }
) }}

select
    event_id,
    event_title,
    event_volume_usd_lifetime_reported
from {{ ref('int_polymarket_wc2026_event_membership') }}
where ever_eligible and membership_status = 'review_required'
