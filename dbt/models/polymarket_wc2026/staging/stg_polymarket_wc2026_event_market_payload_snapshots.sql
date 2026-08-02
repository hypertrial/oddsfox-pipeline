{{ config(tags=['wc2026_logical_atlas']) }}

select
    market_id,
    question,
    category,
    description,
    market_resolution_source,
    outcomes,
    cast(active as boolean) as is_active,
    cast(closed as boolean) as is_closed,
    created_at,
    scraped_at,
    end_date,
    slug,
    event_slug,
    event_id,
    event_title,
    event_start_time,
    event_finished_time,
    event_game_id,
    cast(event_ended as boolean) as event_ended,
    condition_id,
    sports_market_type,
    game_start_time,
    group_item_title,
    group_item_threshold,
    line,
    tags,
    clob_token_ids,
    cast(is_resolved as boolean) as is_resolved,
    winning_outcome,
    winning_clob_token_id,
    cast(neg_risk_other as boolean) as neg_risk_other,
    observed_at,
    case
        when cast(volume as double) >= 0 and isfinite(cast(volume as double))
            then cast(volume as double)
    end as volume,
    nullif(trim(cast(neg_risk_market_id as varchar)), '') as neg_risk_market_id,
    nullif(trim(cast(neg_risk_request_id as varchar)), '') as neg_risk_request_id
from {{ source('polymarket_wc2026_raw', 'event_market_payload_snapshots') }}
