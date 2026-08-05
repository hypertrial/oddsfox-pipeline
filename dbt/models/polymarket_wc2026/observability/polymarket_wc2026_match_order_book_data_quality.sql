{{ config(materialized='table', tags=['pmxt_order_book']) }}

with snapshots as (
    select *
    from {{ ref('stg_polymarket_wc2026_match_order_book_snapshots') }}
),

levels as (
    select *
    from {{ ref('int_polymarket_wc2026_match_order_book_levels') }}
),

issues as (
    select *
    from {{ ref('polymarket_wc2026_match_order_book_quality_issues') }}
)

select
    count(distinct snapshots.scan_id) as published_scans,
    count(distinct snapshots.fifa_match_id) as mapped_games,
    min(snapshots.fifa_match_id) as fifa_match_id,
    count(distinct snapshots.market_id) as mapped_markets,
    count(distinct snapshots.clob_token_id) as mapped_tokens,
    count(
        distinct snapshots.clob_token_id
        || ':'
        || cast(snapshots.snapshot_timestamp_ms as varchar)
        || ':'
        || snapshots.snapshot_sha256
    ) as snapshot_count,
    (select count(*) from levels) as level_count,
    (
        select count(*) from issues
        where issues.severity = 'error'
    ) as error_issue_count,
    (
        select count(*) from issues
        where issues.severity = 'warning'
    ) as warning_issue_count,
    (
        select string_agg(issues.issue_key, ',' order by issues.issue_key)
        from issues
        where issues.severity = 'error'
    ) as blocking_issue_keys
from snapshots
