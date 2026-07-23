{{ config(materialized='table', tags=['polygon_settlement']) }}

select
    *,
    date_diff(
        'minute', analysis_window_start_at_utc, analysis_window_end_at_utc
    ) as window_minutes
from {{ ref('stg_polymarket_wc2026_polygon_settlement_markets') }}
