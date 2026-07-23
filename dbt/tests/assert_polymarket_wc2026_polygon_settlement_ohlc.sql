{{ config(tags=['polygon_settlement']) }}

select
    proposition_id,
    settlement_minute_utc
from {{ ref('polymarket_wc2026_polygon_settlement_minute_odds') }}
where
    (
        yes_observed
        and (
            yes_low > yes_high
            or yes_open not between yes_low and yes_high
            or yes_close not between yes_low and yes_high
            or yes_vwap not between yes_low and yes_high
        )
    )
    or (
        no_observed
        and (
            no_low > no_high
            or no_open not between no_low and no_high
            or no_close not between no_low and no_high
            or no_vwap not between no_low and no_high
        )
    )
