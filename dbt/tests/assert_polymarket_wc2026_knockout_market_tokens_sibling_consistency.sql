select
    market_id,
    clob_token_id,
    outcome_label,
    yes_clob_token_id,
    no_clob_token_id,
    opposite_clob_token_id
from {{ ref('polymarket_wc2026_knockout_market_tokens') }}
where
    lower(outcome_label) in ('yes', 'no')
    and (
        opposite_clob_token_id is null
        or opposite_clob_token_id = clob_token_id
        or (
            lower(outcome_label) = 'yes'
            and clob_token_id is distinct from yes_clob_token_id
        )
        or (
            lower(outcome_label) = 'no'
            and clob_token_id is distinct from no_clob_token_id
        )
    )
