select
    fetch_run_id,
    market_id,
    clobtokenid as clob_token_id,
    exact_window_start_at,
    exact_window_end_at,
    fetch_finished_at,
    in_game_history_sha256
from {{ source('polymarket_soccer_ops', 'match_minute_odds_fetch_audit') }}
where fetch_status = 'success' and raw_published
qualify row_number() over (
    partition by clobtokenid, exact_window_start_at, exact_window_end_at
    order by fetch_finished_at desc, fetch_run_id desc
) = 1
