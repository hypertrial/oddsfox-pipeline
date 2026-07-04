with counts as (
    select count(*) as team_rows
    from {{ ref('international_results_wc2026_team_status') }}
),

stage_counts as (
    select
        stage_key,
        count(*) as stage_rows
    from {{ ref('international_results_wc2026_matches') }}
    group by 1
),

failures as (
    select 'teams' as check_name
    from counts
    where team_rows > 0 and team_rows != 48

    union all

    select 'group_stage_rows' as check_name
    from stage_counts
    where stage_key = 'group_stage' and stage_rows != 72

    union all

    select 'round_of_32_rows' as check_name
    from stage_counts
    where stage_key = 'round_of_32' and stage_rows != 16

    union all

    select 'round_of_16_rows' as check_name
    from stage_counts
    where stage_key = 'round_of_16' and stage_rows not in (0, 8)
)

select check_name
from failures
