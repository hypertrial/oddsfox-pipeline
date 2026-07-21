select
    odds_minute_epoch,
    market_id
from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
where
    minute_complete <> (yes_observed and no_observed)
    or pair_price_anomaly <> coalesce(
        minute_complete and yes_no_close_deviation > 0.05,
        false
    )
    or (minute_complete and minute_status <> 'complete')
    or (not minute_complete and minute_status = 'complete')
    or (not minute_complete and yes_no_close_sum is not null)
    or (not minute_complete and yes_no_close_deviation is not null)
