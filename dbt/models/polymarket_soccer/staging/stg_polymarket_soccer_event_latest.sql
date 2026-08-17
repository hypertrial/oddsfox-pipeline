select
    event_id,
    event_slug,
    event_title,
    event_subtitle,
    series_slugs_json,
    created_at,
    coverage_tier,
    observed_at
from {{ source('polymarket_soccer_raw', 'event_snapshots') }}
qualify row_number() over (
    partition by event_id order by observed_at desc
) = 1
