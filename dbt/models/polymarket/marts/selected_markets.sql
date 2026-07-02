select
    market_id,
    scope_name,
    question,
    description,
    category,
    outcomes,
    volume,
    is_active,
    is_closed,
    created_at,
    scraped_at,
    end_date,
    slug,
    event_slug
from {{ ref('int_polymarket_selected_markets') }}
