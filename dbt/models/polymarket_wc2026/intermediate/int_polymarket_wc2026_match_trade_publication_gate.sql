{{ config(materialized='table', tags=['market_portrait']) }}

with latest as (
    select *
    from {{ source('polymarket_wc2026_ops', 'match_trade_scan_runs') }}
    where status = 'published'
    qualify row_number() over (order by finished_at desc, scan_id desc) = 1
),

raw_stats as (
    select
        latest.scan_id,
        count(trades.trade_id) as raw_trade_count,
        count(*) filter (
            where
            trades.trade_id is not null
            and (
                not regexp_full_match(
                    trades.price,
                    '(0|[1-9][0-9]{0,19})([.][0-9]{1,18})?'
                )
                or try_cast(trades.price as decimal(38, 18)) not between 0 and 1
                or not regexp_full_match(
                    trades.amount,
                    '(0|[1-9][0-9]{0,19})([.][0-9]{1,18})?'
                )
                or try_cast(trades.amount as decimal(38, 18)) <= 0
            )
        ) as numeric_issue_count,
        count(distinct trades.manifest_sha256) as manifest_count,
        min(trades.manifest_sha256) as manifest_sha256,
        sha256(
            string_agg(
                trades.trade_id,
                chr(10)
                order by
                    trades.clob_token_id,
                    trades.trade_timestamp_ms,
                    trades.event_sequence,
                    trades.trade_id
            )
        ) as recomputed_sha256
    from latest
    left join {{ source('polymarket_wc2026_raw', 'match_trades') }} as trades
        on latest.scan_id = trades.scan_id
    group by latest.scan_id
),

window_stats as (
    select
        latest.scan_id,
        count(windows.clob_token_id) as window_count,
        count(*) filter (where windows.status in ('pending', 'failed'))
            as unfinished_window_count
    from latest
    left join {{ source('polymarket_wc2026_ops', 'match_trade_scan_windows') }} as windows
        on latest.scan_id = windows.scan_id
    group by latest.scan_id
),

sequence_issues as (
    select count(*) as issue_count
    from (
        select trades.clob_token_id
        from {{ source('polymarket_wc2026_raw', 'match_trades') }} as trades
        inner join latest on trades.scan_id = latest.scan_id
        group by trades.clob_token_id
        having
            min(trades.event_sequence) != 0
            or max(trades.event_sequence) != count(*) - 1
            or count(distinct trades.event_sequence) != count(*)
    ) as invalid_sequences
)

select
    latest.scan_id,
    case
        when
            latest.scan_id is not null
            and latest.trade_count > 0
            and latest.aggregate_sha256 is not null
            and latest.trade_count = raw_stats.raw_trade_count
            and latest.aggregate_sha256 = raw_stats.recomputed_sha256
            and raw_stats.numeric_issue_count = 0
            and raw_stats.manifest_count = 1
            and raw_stats.manifest_sha256 = latest.manifest_sha256
            and window_stats.window_count > 0
            and window_stats.unfinished_window_count = 0
            and sequence_issues.issue_count = 0
            then true
        else error('WC2026 PMXT trade publication gate failed')
    end as publication_ready
from latest
inner join raw_stats on latest.scan_id = raw_stats.scan_id
inner join window_stats on latest.scan_id = window_stats.scan_id
cross join sequence_issues
