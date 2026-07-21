with match_31 as (
    select
        kickoff_at_et,
        timezone('America/New_York', kickoff_at_et) at time zone 'UTC'
            as kickoff_at_utc
    from {{ ref('wc2026_fixtures') }}
    where match_id = 31
),

summary as (
    select
        count(*) as row_count,
        min(kickoff_at_et) as kickoff_at_et,
        min(kickoff_at_utc) as kickoff_at_utc
    from match_31
)

select *
from summary
where
    row_count <> 1
    or kickoff_at_et <> timestamp '2026-06-19 23:00:00'
    or kickoff_at_utc <> timestamp '2026-06-20 03:00:00'
