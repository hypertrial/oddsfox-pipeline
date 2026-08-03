{{ config(materialized='table', tags=['market_portrait']) }}

select trades.*
from {{ source('polymarket_wc2026_raw', 'match_trades') }} as trades
cross join {{ ref('int_polymarket_wc2026_match_trade_publication_gate') }} as gate
where
    trades.scan_id = gate.scan_id
    and gate.publication_ready
order by
    trades.trade_timestamp_ms,
    trades.event_sequence,
    trades.trade_id
