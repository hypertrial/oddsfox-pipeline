select
    clob_token_id,
    market_id,
    market_direction,
    source_outcome_label,
    price_represents,
    progression_outcome_label
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
    or price_represents != 'progression'
    or (
        market_direction = 'elimination'
        and stage_key = 'round_of_32'
        and progression_outcome_label != 'not_eliminated_in_round_of_32'
    )
    or (
        market_direction = 'elimination'
        and stage_key = 'round_of_16'
        and progression_outcome_label != 'not_eliminated_in_round_of_16'
    )
    or market_direction not in ('winner', 'advance', 'elimination')
