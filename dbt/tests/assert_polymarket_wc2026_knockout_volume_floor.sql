select
    tokens.clob_token_id,
    tokens.market_id,
    tokens.market_volume_usd
from {{ ref('polymarket_wc2026_knockout_market_tokens') }} as tokens
-- costguard: allow cross-join, WC2026 contract seed has one row.
cross join {{ ref('polymarket_wc2026_contract') }} as contract
where
    contract.scope_name = 'wc2026'
    and coalesce(tokens.market_volume_usd, 0) < contract.knockout_min_volume_usd
