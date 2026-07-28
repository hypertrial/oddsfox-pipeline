{{ config(materialized='table', tags=['pmxt_order_book']) }}

with quality as (
    select
        count(*) filter (where severity = 'error') as error_issue_count,
        count(*) filter (where severity = 'warning') as warning_issue_count,
        string_agg(issue_key, ',' order by issue_key)
        filter (where severity = 'error') as blocking_issue_keys
    from {{ ref('polymarket_wc2026_match_order_book_quality_issues') }}
),

inventory as (
    select count(*) as snapshot_count
    from {{ ref('stg_polymarket_wc2026_match_order_book_snapshots') }}
)

select
    quality.error_issue_count,
    quality.warning_issue_count,
    case
        when quality.error_issue_count = 0 and inventory.snapshot_count > 0
            then true
        else error(
            'WC2026 PMXT order-book publication blocked: '
            || coalesce(quality.blocking_issue_keys, 'empty_inventory')
        )
    end as publication_ready
from quality
cross join inventory
