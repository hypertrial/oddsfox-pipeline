{{ config(tags=['polygon_settlement']) }}

with inventory as (
    select
        count(*) as minute_rows,
        count(distinct proposition_id) as propositions,
        count(distinct fifa_match_id) as games,
        min(fifa_match_id) as first_match_id,
        max(fifa_match_id) as last_match_id
    from {{ ref('polymarket_wc2026_polygon_settlement_minute_odds') }}
)

select *
from inventory
where
    minute_rows <> 39120
    or propositions <> 248
    or games <> 104
    or first_match_id <> 1
    or last_match_id <> 104
