-- Keep odds staging inside the payload-backed token catalog
-- (stg_polymarket_wc2026_market_tokens). Raw history can retain rows for
-- registry/enrichment tokens that never landed in event_market_payload_snapshots.
select
    odds.clobtokenid as clob_token_id,
    odds.timestamp as odds_timestamp_epoch,
    odds.price,
    odds.ingested_at,
    to_timestamp(odds.timestamp) at time zone 'UTC' as odds_timestamp
from {{ source('polymarket_wc2026_raw', 'odds_history') }} as odds
inner join {{ ref('stg_polymarket_wc2026_market_tokens') }} as tokens
    on odds.clobtokenid = tokens.clob_token_id
