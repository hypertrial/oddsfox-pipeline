with expected as (
    select fifa_match_id
    from unnest(list_concat(range(73, 103), [104])) as expected_ids (fifa_match_id)
),

actual as (
    select fifa_match_id
    from {{ ref('int_wc2026_knockout_fixtures') }}
),

mismatches as (
    select
        e.fifa_match_id as expected_fifa_match_id,
        a.fifa_match_id as actual_fifa_match_id
    from expected as e
    full outer join actual as a
        on e.fifa_match_id = a.fifa_match_id
    where e.fifa_match_id is null or a.fifa_match_id is null
)

select *
from mismatches
where exists (select 1 from actual)
