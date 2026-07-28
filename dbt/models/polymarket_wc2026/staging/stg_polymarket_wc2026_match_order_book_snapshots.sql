{{ config(tags=['pmxt_order_book']) }}

with latest_published as (
    select
        scan_id,
        row_number() over (
            order by finished_at desc, scan_id desc
        ) as publication_rank
    from {{ source('polymarket_wc2026_ops', 'match_order_book_scan_runs') }}
    where status = 'published' and raw_published
)

select
    snapshots.scan_id,
    snapshots.manifest_sha256,
    snapshots.stage,
    snapshots.home_team,
    snapshots.away_team,
    snapshots.event_id,
    snapshots.event_slug,
    snapshots.market_id,
    snapshots.market_slug,
    snapshots.market_type,
    snapshots.outcome_label,
    snapshots.landscape_role,
    snapshots.clob_token_id,
    snapshots.bids_json,
    snapshots.asks_json,
    snapshots.is_neg_risk,
    snapshots.source_endpoint,
    snapshots.last_trade_price as last_trade_price_raw,
    cast(snapshots.fifa_match_id as bigint) as fifa_match_id,
    cast(snapshots.window_start_ms as bigint) as window_start_ms,
    cast(snapshots.window_end_ms as bigint) as window_end_ms,
    cast(snapshots.snapshot_timestamp_ms as bigint) as snapshot_timestamp_ms,
    cast(snapshots.snapshot_at as timestamp) as snapshot_at_utc,
    cast(snapshots.ingested_at as timestamp) as ingested_at,
    cast(snapshots.provider_sequence as bigint) as provider_sequence,
    lower(snapshots.condition_id) as condition_id,
    lower(snapshots.snapshot_sha256) as snapshot_sha256,
    json_valid(snapshots.bids_json) as bids_json_valid,
    json_valid(snapshots.asks_json) as asks_json_valid,
    try_cast(snapshots.last_trade_price as decimal(38, 18)) as last_trade_price
from {{ source('polymarket_wc2026_raw', 'match_order_book_snapshots') }} as snapshots
inner join latest_published as published
    on snapshots.scan_id = published.scan_id
where published.publication_rank = 1
