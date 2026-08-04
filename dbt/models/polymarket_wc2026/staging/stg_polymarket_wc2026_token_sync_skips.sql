select
    skips.clobtokenid as clob_token_id,
    skips.reason,
    skips.created_at
from {{ source('polymarket_wc2026_ops', 'token_sync_skips') }} as skips
inner join {{ ref('stg_polymarket_wc2026_market_tokens') }} as tokens
    on skips.clobtokenid = tokens.clob_token_id
