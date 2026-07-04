select
    clob_token_id,
    odds_hour_utc
from {{ ref('polymarket_wc2026_knockout_token_hourly_odds') }}
where odds_hour_utc < current_timestamp - interval 30 day
