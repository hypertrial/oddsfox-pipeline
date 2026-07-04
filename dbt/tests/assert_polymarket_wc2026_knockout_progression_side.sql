select
    clob_token_id,
    market_id,
    market_direction,
    source_outcome_label
from {{ ref('polymarket_wc2026_knockout_market_tokens') }}
where
    (
        market_direction in ('winner', 'advance')
        and lower(source_outcome_label) != 'yes'
    )
    or (
        market_direction = 'elimination'
        and lower(source_outcome_label) != 'no'
    )
    or market_direction not in ('winner', 'advance', 'elimination')
