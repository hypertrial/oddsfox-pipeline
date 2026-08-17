{{ config(materialized='table') }}

select *
from {{ ref('int_polymarket_soccer_match_result_observed') }}
