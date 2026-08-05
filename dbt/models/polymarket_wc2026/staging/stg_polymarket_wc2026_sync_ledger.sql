select
    ledger.clobtokenid as clob_token_id,
    ledger.last_sync_timestamp,
    ledger.last_checked_at,
    ledger.next_check_at,
    cast(ledger.fully_checked as boolean) as is_fully_checked,
    to_timestamp(ledger.last_sync_timestamp) at time zone 'UTC' as last_sync_at,
    coalesce(ledger.empty_run_streak, 0) as empty_run_streak
from {{ source('polymarket_wc2026_ops', 'token_sync_ledger') }} as ledger
inner join {{ ref('stg_polymarket_wc2026_market_tokens') }} as tokens
    on ledger.clobtokenid = tokens.clob_token_id
