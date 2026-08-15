{{ config(tags=['minute_odds']) }}

-- Keep the public mart view in the +DQ selection graph without scanning it here.
{% do ref('polymarket_wc2026_market_minute_odds') %}

with fact as (
    select
        count(*) as mart_rows,
        count(distinct market_id) as mart_markets,
        count(distinct clob_token_id) as mart_tokens,
        count(*) filter (where minute_source = 'match') as match_source_rows,
        count(*) filter (where minute_source = 'futures') as futures_source_rows,
        count(distinct clob_token_id) filter (
            where minute_source = 'match'
        ) as match_primary_tokens_with_prices,
        count(distinct clob_token_id) filter (
            where minute_source = 'futures'
        ) as futures_primary_tokens_with_prices,
        count(*) filter (
            where
            open_price is null
            or high_price is null
            or low_price is null
            or close_price is null
        ) as null_ohlc_rows,
        count(*) filter (
            where not (low_price <= open_price and open_price <= high_price)
            or not (low_price <= close_price and close_price <= high_price)
        ) as ohlc_order_issues
    from {{ ref('int_polymarket_wc2026_token_minute_odds') }}
),

latest_futures_audit as (
    select
        count(*) as latest_audit_rows,
        count(*) filter (
            where source_audit.fetch_status = 'success'
        ) as latest_success_rows,
        count(*) filter (
            where source_audit.fetch_status = 'empty'
        ) as latest_empty_rows,
        count(*) filter (
            where source_audit.fetch_status in ('error', 'cancelled')
        ) as latest_hard_failure_rows,
        count(*) filter (
            where source_audit.raw_published
        ) as latest_published_rows
    from {{ ref('stg_polymarket_wc2026_futures_minute_fetch_audit') }} as source_audit
    where source_audit.fetch_run_id = (
        select latest.fetch_run_id
        from {{ ref('stg_polymarket_wc2026_futures_minute_fetch_audit') }} as latest
        order by latest.fetch_finished_at desc
        limit 1
    )
),

checks as (
    select
        fact.*,
        latest_futures_audit.latest_audit_rows,
        latest_futures_audit.latest_success_rows,
        latest_futures_audit.latest_empty_rows,
        latest_futures_audit.latest_hard_failure_rows,
        latest_futures_audit.latest_published_rows,
        -- Compatibility aliases for existing integration assertions.
        fact.match_primary_tokens_with_prices as match_tokens_with_prices,
        fact.futures_primary_tokens_with_prices as futures_tokens_with_prices,
        fact.mart_rows > 0 as has_mart_rows,
        fact.mart_markets > 0 as has_mart_markets,
        fact.null_ohlc_rows = 0 as ohlc_complete,
        fact.ohlc_order_issues = 0 as ohlc_ordered,
        fact.match_source_rows > 0 as has_match_rows,
        fact.futures_source_rows > 0 as has_futures_rows,
        (
            latest_futures_audit.latest_audit_rows > 0
            and latest_futures_audit.latest_success_rows > 0
            and latest_futures_audit.latest_hard_failure_rows = 0
            and latest_futures_audit.latest_published_rows
            = latest_futures_audit.latest_success_rows
        ) as futures_audit_healthy
    from fact
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
