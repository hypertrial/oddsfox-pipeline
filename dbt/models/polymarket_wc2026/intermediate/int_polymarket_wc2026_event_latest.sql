select
    event_id,
    event_slug,
    event_title,
    event_description,
    event_volume_usd_lifetime_reported,
    volume_24h_usd,
    volume_1w_usd,
    volume_1m_usd,
    volume_1y_usd,
    liquidity_usd,
    is_active,
    is_closed,
    start_at,
    end_at,
    event_start_at,
    finished_at,
    tags_json,
    observed_at
from {{ ref('stg_polymarket_wc2026_event_snapshots') }}
qualify row_number() over (
    partition by event_id
    order by observed_at desc
) = 1
