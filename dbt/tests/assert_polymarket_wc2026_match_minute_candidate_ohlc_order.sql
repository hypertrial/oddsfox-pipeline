select
    odds_minute_epoch,
    market_id
from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
where
    (
        yes_observed
        and (
            yes_low_price > yes_high_price
            or yes_open_price not between yes_low_price and yes_high_price
            or yes_close_price not between yes_low_price and yes_high_price
            or yes_average_price not between yes_low_price and yes_high_price
        )
    )
    or (
        no_observed
        and (
            no_low_price > no_high_price
            or no_open_price not between no_low_price and no_high_price
            or no_close_price not between no_low_price and no_high_price
            or no_average_price not between no_low_price and no_high_price
        )
    )
