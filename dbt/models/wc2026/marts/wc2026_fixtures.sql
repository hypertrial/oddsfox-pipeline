{{ config(alias='fixtures') }}

select
    cast(match_id as integer) as match_id,
    cast(stage as varchar) as stage, -- noqa: RF04
    cast(nullif(trim(group_label), '') as varchar) as group_label,
    cast(match_date as date) as match_date,
    cast(nullif(trim(kickoff_time_et), '') as varchar) as kickoff_time_et,
    cast(nullif(trim(venue), '') as varchar) as venue,
    cast(nullif(trim(home_team), '') as varchar) as home_team,
    cast(nullif(trim(away_team), '') as varchar) as away_team,
    cast(nullif(trim(home_slot), '') as varchar) as home_slot,
    cast(nullif(trim(away_slot), '') as varchar) as away_slot,
    cast(nullif(trim(status), '') as varchar) as status,
    cast(source as varchar) as source_provenance,
    try_cast(nullif(trim(cast(matchday as varchar)), '') as integer) as matchday,
    try_cast(
        strptime(
            cast(match_date as varchar) || ' ' || cast(kickoff_time_et as varchar),
            '%Y-%m-%d %I:%M %p'
        )
        as timestamp
    ) as kickoff_at_et,
    case stage
        when 'Group Stage' then 1
        when 'Round of 32' then 2
        when 'Round of 16' then 3
        when 'Quarter-final' then 4
        when 'Semi-final' then 5
        when 'Third-place' then 6
        when 'Final' then 7
    end as stage_order,
    stage <> 'Group Stage' as is_knockout
from {{ ref('wc2026_schedule_matches') }}
