{{ config(materialized='table', tags=['pmxt_order_book', 'market_portrait']) }}

with deduplicated as (
    select snapshots.*
    from {{ ref('stg_polymarket_wc2026_match_order_book_snapshots') }} as snapshots
    cross join {{ ref('int_polymarket_wc2026_match_order_book_publication_gate') }} as gate
    where gate.publication_ready
    qualify row_number() over (
        partition by
            snapshots.clob_token_id,
            snapshots.snapshot_timestamp_ms,
            snapshots.snapshot_sha256
        order by snapshots.provider_sequence
    ) = 1
)

select
    deduplicated.*,
    row_number() over (
        partition by deduplicated.clob_token_id
        order by
            deduplicated.snapshot_timestamp_ms,
            deduplicated.provider_sequence,
            deduplicated.snapshot_sha256
    ) - 1 as event_sequence
from deduplicated
