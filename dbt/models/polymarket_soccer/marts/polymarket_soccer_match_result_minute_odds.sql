{{ config(materialized='view') }}

select * exclude (source_revision)
from {{ ref('int_polymarket_soccer_match_result_minute_odds') }}
