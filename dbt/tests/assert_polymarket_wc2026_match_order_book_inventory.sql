{{ config(tags=['pmxt_order_book']) }}

select *
from {{ ref('polymarket_wc2026_match_order_book_data_quality') }}
where
    mapped_games != 1
    or mapped_markets != 1
    or mapped_tokens != 2
    or snapshot_count = 0
    or error_issue_count != 0
