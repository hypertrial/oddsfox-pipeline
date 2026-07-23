select *
from {{ ref('wc2026_fixtures') }}
where
    match_date is null
    or kickoff_time_et is null
    or kickoff_at_et is null
