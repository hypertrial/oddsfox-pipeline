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
),

audit_history as (
    select
        market_id,
        clobtokenid as clob_token_id,
        exact_window_start_at,
        exact_window_end_at,
        count(*) as fetch_attempts,
        min(fetch_finished_at) filter (
            where fetch_status in ('error', 'cancelled', 'empty')
        ) as first_unavailable_at,
        max(fetch_finished_at) filter (
            where fetch_status in ('error', 'cancelled', 'empty')
        ) as latest_unavailable_at,
        max(fetch_finished_at) as latest_attempt_at
    from {{ source('polymarket_soccer_ops', 'match_minute_odds_fetch_audit') }}
    group by all
)

select
    tokens.*,
    fetch_audit.fetch_status,
    fetch_audit.fetch_finished_at,
    terminal.terminal_at,
    terminal.empty_retry_hours,
    audit_history.fetch_attempts,
    audit_history.first_unavailable_at,
    audit_history.latest_unavailable_at,
    audit_history.latest_attempt_at,
    coalesce(fetch_audit.raw_published, false) as raw_published,
    fetch_audit.fetch_status in ('error', 'cancelled')
    or (
        fetch_audit.fetch_status = 'empty'
        and terminal.clob_token_id is null
    ) as is_retry_backlog,
    fetch_audit.fetch_status = 'empty'
    and terminal.clob_token_id is not null
        as is_terminal_unavailable,
    case
        when
            fetch_audit.fetch_status in ('error', 'cancelled')
            or (fetch_audit.fetch_status = 'empty' and terminal.clob_token_id is null)
            then date_diff('hour', audit_history.first_unavailable_at, current_timestamp)
    end as retry_age_hours
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
left join audit_history
    on
        tokens.market_id = audit_history.market_id
        and tokens.clob_token_id = audit_history.clob_token_id
        and tokens.window_start_at = audit_history.exact_window_start_at
        and tokens.window_end_at = audit_history.exact_window_end_at
