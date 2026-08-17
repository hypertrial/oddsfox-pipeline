{{ config(materialized='table') }}

with tokens as (
    select
        event_id,
        market_id,
        result_role,
        window_start_at,
        window_end_at,
        unnest([yes_token_id, no_token_id]) as clob_token_id
    from {{ ref('stg_polymarket_soccer_match_result_registry') }}
)

select
    tokens.*,
    fetch_audit.fetch_status,
    fetch_audit.fetch_finished_at,
    terminal.terminal_at,
    terminal.empty_retry_hours,
    coalesce(fetch_audit.raw_published, false) as raw_published,
    fetch_audit.fetch_status in ('error', 'cancelled')
    or (
        fetch_audit.fetch_status = 'empty'
        and terminal.clob_token_id is null
    ) as is_retry_backlog,
    fetch_audit.fetch_status = 'empty'
    and terminal.clob_token_id is not null
        as is_terminal_unavailable
from tokens
left join {{ ref('stg_polymarket_soccer_match_minute_audit_latest') }} as fetch_audit
    on
        tokens.market_id = fetch_audit.market_id
        and tokens.clob_token_id = fetch_audit.clob_token_id
        and tokens.window_start_at = fetch_audit.exact_window_start_at
        and tokens.window_end_at = fetch_audit.exact_window_end_at
left join {{ source('polymarket_soccer_ops', 'match_minute_odds_terminal_unavailable') }} as terminal
    on
        tokens.market_id = terminal.market_id
        and tokens.clob_token_id = terminal.clob_token_id
        and tokens.window_start_at = terminal.exact_window_start_at
        and tokens.window_end_at = terminal.exact_window_end_at
