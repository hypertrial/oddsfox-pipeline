select
    clobtokenid as clob_token_id,
    reason,
    created_at
from {{ source('polymarket_us_midterms_2026_ops', 'token_sync_skips') }}
