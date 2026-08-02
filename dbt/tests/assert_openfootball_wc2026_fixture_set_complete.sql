{{ config(tags=['wc2026_logical_atlas']) }}

with expected as (
    select cast(range as integer) as fifa_match_id
    from range(1, 105)
),

actual as (
    select fifa_match_id
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }}
),

missing as (
    select expected.fifa_match_id
    from expected
    left join actual on expected.fifa_match_id = actual.fifa_match_id
    where actual.fifa_match_id is null
),

unexpected_or_duplicate as (
    select fifa_match_id
    from actual
    group by fifa_match_id
    having
        fifa_match_id not between 1 and 104
        or count(*) != 1
)

select
    fifa_match_id,
    'missing' as issue
from missing
union all
select
    fifa_match_id,
    'unexpected_or_duplicate' as issue
from unexpected_or_duplicate
