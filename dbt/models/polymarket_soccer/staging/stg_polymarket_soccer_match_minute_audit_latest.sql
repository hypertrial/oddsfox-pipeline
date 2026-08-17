select
    market_id,
    clobtokenid as clob_token_id,
    fetch_status,
    raw_published,
    exact_window_start_at,
    exact_window_end_at,
    fetch_finished_at
from {{ source('polymarket_soccer_ops', 'match_minute_odds_fetch_audit') }}
qualify row_number() over (
    partition by clobtokenid, exact_window_start_at, exact_window_end_at
    order by fetch_finished_at desc, fetch_run_id desc
) = 1
