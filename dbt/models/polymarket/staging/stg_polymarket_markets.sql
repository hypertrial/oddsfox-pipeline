select
    id as market_id,
    question,
    category,
    description,
    outcomes,
    volume,
    cast(active as boolean) as is_active,
    cast(closed as boolean) as is_closed,
    created_at,
    scraped_at,
    end_date,
    slug,
    event_slug,
    event_id
from {{ source('polymarket_raw', 'markets') }}
