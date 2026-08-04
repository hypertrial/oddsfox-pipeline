-- Keep odds staging inside the payload-backed token catalog
-- (stg_polymarket_wc2026_market_tokens). Raw history can retain rows for
-- registry/enrichment tokens that never landed in event_market_payload_snapshots.
select
    odds.CLOBTOKENID as CLOB_TOKEN_ID,
    odds.TIMESTAMP as ODDS_TIMESTAMP_EPOCH,
    odds.PRICE,
    odds.INGESTED_AT,
    to_timestamp(odds.TIMESTAMP) as ODDS_TIMESTAMP
from {{ source('polymarket_wc2026_raw', 'odds_history') }} as odds
inner join {{ ref('stg_polymarket_wc2026_market_tokens') }} as tokens
    on odds.CLOBTOKENID = tokens.clob_token_id
