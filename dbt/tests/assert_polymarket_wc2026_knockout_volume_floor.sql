select
    clob_token_id,
    market_id,
    market_volume_usd
from {{ ref('polymarket_wc2026_knockout_market_tokens') }}
where coalesce(market_volume_usd, 0) < 5000
