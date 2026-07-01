select
    market_id,
    question,
    category,
    description,
    outcomes,
    volume,
    is_active,
    is_closed,
    created_at,
    scraped_at,
    end_date,
    slug,
    event_slug,
    event_id,
    is_wc2026_target
from {{ ref('stg_polymarket_markets') }}
where is_wc2026_target
