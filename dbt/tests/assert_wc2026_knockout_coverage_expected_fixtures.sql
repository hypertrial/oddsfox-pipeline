with expected as (
    select fifa_match_id
    from unnest(list_concat(range(73, 103), [104])) as expected_ids (fifa_match_id)
),

actual as (
    select fifa_match_id
    from {{ ref('wc2026_knockout_match_odds_coverage') }}
)

select
    e.fifa_match_id as expected_fifa_match_id,
    a.fifa_match_id as actual_fifa_match_id
from expected as e
full outer join actual as a
    on e.fifa_match_id = a.fifa_match_id
where e.fifa_match_id is null or a.fifa_match_id is null
