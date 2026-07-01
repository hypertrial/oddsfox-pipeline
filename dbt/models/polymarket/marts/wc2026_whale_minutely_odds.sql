select
    market_id,
    outcome_index,
    clob_token_id,
    question,
    event_slug,
    is_active,
    is_closed,
    market_volume_usd,
    odds_timestamp,
    odds_timestamp_epoch,
    price
from {{ ref('wc2026_token_minutely_odds') }}
where coalesce(market_volume_usd, 0) >= {{ var('polymarket_whale_min_volume_usd', 100000) }}
