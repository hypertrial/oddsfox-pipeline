{{ config(tags=['minute_odds']) }}

with futures_tokens as (
    select count(distinct clob_token_id) as futures_tokens_with_prices
    from {{ ref('int_polymarket_wc2026_futures_token_minute_odds') }}
),

match_tokens as (
    select count(distinct clob_token_id) as match_tokens_with_prices
    from {{ ref('int_polymarket_wc2026_match_token_minute_odds') }}
),

unified as (
    select
        count(*) as mart_rows,
        count(distinct market_id) as mart_markets,
        count(distinct clob_token_id) as mart_tokens,
        count(*) filter (where minute_source = 'match') as match_source_rows,
        count(*) filter (where minute_source = 'futures') as futures_source_rows,
        count(*) filter (
            where
            open_odds is null
            or high_odds is null
            or low_odds is null
            or close_odds is null
        ) as null_ohlc_rows,
        count(*) filter (
            where not (low_odds <= open_odds and open_odds <= high_odds)
            or not (low_odds <= close_odds and close_odds <= high_odds)
        ) as ohlc_order_issues
    from {{ ref('polymarket_wc2026_market_minute_odds') }}
),

latest_futures_audit as (
    select
        count(*) as latest_audit_rows,
        count(*) filter (where fetch_status = 'success') as latest_success_rows,
        count(*) filter (where fetch_status = 'empty') as latest_empty_rows,
        count(*) filter (
            where fetch_status in ('error', 'cancelled')
        ) as latest_hard_failure_rows,
        count(*) filter (where raw_published) as latest_published_rows
    from {{ ref('stg_polymarket_wc2026_futures_minute_fetch_audit') }}
    where fetch_run_id = (
        select fetch_run_id
        from {{ ref('stg_polymarket_wc2026_futures_minute_fetch_audit') }}
        order by fetch_finished_at desc
        limit 1
    )
),

checks as (
    select
        unified.*,
        futures_tokens.futures_tokens_with_prices,
        match_tokens.match_tokens_with_prices,
        latest_futures_audit.latest_audit_rows,
        latest_futures_audit.latest_success_rows,
        latest_futures_audit.latest_empty_rows,
        latest_futures_audit.latest_hard_failure_rows,
        latest_futures_audit.latest_published_rows,
        unified.mart_rows > 0 as has_mart_rows,
        unified.mart_markets > 0 as has_mart_markets,
        unified.null_ohlc_rows = 0 as ohlc_complete,
        unified.ohlc_order_issues = 0 as ohlc_ordered,
        unified.match_source_rows > 0 as has_match_rows,
        unified.futures_source_rows > 0 as has_futures_rows,
        (
            latest_futures_audit.latest_audit_rows > 0
            and latest_futures_audit.latest_success_rows > 0
            and latest_futures_audit.latest_hard_failure_rows = 0
            and latest_futures_audit.latest_published_rows
            = latest_futures_audit.latest_success_rows
        ) as futures_audit_healthy
    from unified
    cross join futures_tokens
    cross join match_tokens
    cross join latest_futures_audit
)

select
    *,
    nullif(
        concat_ws(
            ',',
            case when not has_mart_rows then 'mart_rows' end,
            case when not has_mart_markets then 'mart_markets' end,
            case when not ohlc_complete then 'ohlc_complete' end,
            case when not ohlc_ordered then 'ohlc_ordered' end,
            case when not has_match_rows then 'match_rows' end,
            case when not has_futures_rows then 'futures_rows' end,
            case when not futures_audit_healthy then 'futures_audit' end
        ),
        ''
    ) as blocking_issue_keys
from checks
