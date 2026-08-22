select *
from {{ ref('polymarket_soccer_match_result_minute_odds') }}
where
    open_odds not between 0 and 1
    or high_odds not between 0 and 1
    or low_odds not between 0 and 1
    or close_odds not between 0 and 1
    or avg_odds not between 0 and 1
    or (
        no_open_odds is not null
        and (
            no_open_odds not between 0 and 1
            or no_high_odds not between 0 and 1
            or no_low_odds not between 0 and 1
            or no_close_odds not between 0 and 1
            or no_avg_odds not between 0 and 1
        )
    )
