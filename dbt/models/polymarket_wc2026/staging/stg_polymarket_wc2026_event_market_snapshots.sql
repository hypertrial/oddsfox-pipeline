select
    event_id,
    market_id,
    source_ordinal,
    is_enclosing_event,
    observed_at
from {{ source('polymarket_wc2026_raw', 'event_market_snapshots') }}
