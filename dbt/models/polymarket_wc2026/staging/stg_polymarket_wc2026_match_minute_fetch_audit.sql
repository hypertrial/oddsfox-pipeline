{{ config(tags=['cross_domain']) }}

select
    fetch_run_id,
    market_id,
    clobtokenid as clob_token_id,
    fetch_status,
    raw_published,
    cast(fidelity_minutes as integer) as fidelity_minutes,
    exact_window_start_at,
    exact_window_end_at,
    cast(request_start_epoch as bigint) as request_start_epoch,
    cast(request_end_epoch as bigint) as request_end_epoch,
    cast(source_row_count as bigint) as source_row_count,
    cast(in_game_row_count as bigint) as in_game_row_count,
    in_game_history_sha256,
    source_endpoint,
    fetch_started_at,
    fetch_finished_at,
    error_type,
    error_message
from {{ source('polymarket_wc2026_ops', 'match_minute_odds_fetch_audit') }}
