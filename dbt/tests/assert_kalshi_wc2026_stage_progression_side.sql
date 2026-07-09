select
    market_ticker,
    market_direction,
    price_represents,
    progression_outcome_label,
    stage_key
from {{ ref('kalshi_wc2026_stage_markets') }}
where
    price_represents != 'progression'
    or market_direction not in ('winner', 'elimination')
    or (
        market_direction = 'winner'
        and (
            stage_key != 'winner'
            or progression_outcome_label != 'win_world_cup'
        )
    )
    or (
        market_direction = 'elimination'
        and stage_key = 'group_stage'
        and progression_outcome_label != 'not_eliminated_in_group_stage'
    )
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
