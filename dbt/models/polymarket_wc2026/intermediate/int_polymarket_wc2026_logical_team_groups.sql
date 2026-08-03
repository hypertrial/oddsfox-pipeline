{{ config(tags=['wc2026_logical_atlas']) }}

with team_group_candidates as (
    select
        lower(fixtures.group_label) as group_label,
        fixtures.home_team as team_name
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }} as fixtures
    where fixtures.group_label is not null

    union all

    select
        lower(fixtures.group_label) as group_label,
        fixtures.away_team as team_name
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }} as fixtures
    where fixtures.group_label is not null
)

select
    identities.canonical_team_id,
    min(candidates.group_label) as group_label
from team_group_candidates as candidates
inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as identities
    on
        identities.team_match_key
        = {{ canonical_team_match_key('candidates.team_name') }}
group by identities.canonical_team_id
having count(distinct candidates.group_label) = 1
