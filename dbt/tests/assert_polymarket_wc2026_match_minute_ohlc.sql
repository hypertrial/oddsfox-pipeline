select
    clob_token_id,
    odds_minute_epoch
from {{ ref('int_polymarket_wc2026_match_token_minute_odds') }}
where
    low_price > least(open_price, close_price, average_price)
    or high_price < greatest(open_price, close_price, average_price)
    or low_price > high_price
