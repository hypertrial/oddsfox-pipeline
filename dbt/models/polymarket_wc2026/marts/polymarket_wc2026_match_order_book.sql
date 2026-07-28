{{ config(materialized='table', tags=['pmxt_order_book']) }}

select
    levels.scan_id as published_scan_id,
    levels.manifest_sha256,
    levels.fifa_match_id,
    levels.stage,
    levels.home_team,
    levels.away_team,
    levels.event_id,
    levels.event_slug,
    levels.market_id,
    levels.market_slug,
    levels.market_type,
    levels.condition_id,
    levels.outcome_label,
    levels.clob_token_id,
    levels.snapshot_timestamp_ms,
    levels.snapshot_at_utc,
    levels.snapshot_sha256,
    levels.book_side,
    levels.level_rank,
    levels.price,
    levels.size,
    levels.order_count,
    levels.level_notional,
    levels.cumulative_size,
    levels.cumulative_notional,
    levels.best_bid_price,
    levels.best_ask_price,
    levels.spread,
    levels.midpoint,
    levels.last_trade_price,
    levels.is_neg_risk,
    levels.source_endpoint as source_label,
    levels.ingested_at
from {{ ref('int_polymarket_wc2026_match_order_book_levels') }} as levels
cross join {{ ref('int_polymarket_wc2026_match_order_book_publication_gate') }} as gate
where gate.publication_ready
