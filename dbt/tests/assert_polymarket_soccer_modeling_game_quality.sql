select *
from {{ ref('int_polymarket_soccer_match_result_modeling_games') }}
where
    observed_minutes * 100 < expected_minutes * 99
    or maximum_consecutive_gap_minutes > 3
