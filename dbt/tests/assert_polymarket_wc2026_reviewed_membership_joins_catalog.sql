{{ config(
    tags = ['wc2026_logical_atlas'],
    meta = {
        'dagster': {
            'ref': {'name': 'int_polymarket_wc2026_event_membership'},
            'asset_key': [
                'polymarket', 'wc2026', 'intermediate', 'event_membership'
            ]
        }
    }
) }}

select reviews.event_id
from {{ source('polymarket_wc2026_raw', 'reviewed_event_membership') }} as reviews
left join {{ ref('int_polymarket_wc2026_event_latest') }} as events
    on reviews.event_id = events.event_id
where events.event_id is null
