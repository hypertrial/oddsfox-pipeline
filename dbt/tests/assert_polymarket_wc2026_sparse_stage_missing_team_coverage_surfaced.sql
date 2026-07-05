{{ config(
    severity = 'warn',
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_knockout_data_quality'},
            'asset_key': ['polymarket', 'wc2026', 'observability', 'knockout_data_quality']
        }
    }
) }}

with contract as (
    select
        knockout_min_volume_usd,
        round_of_32_min_raw_markets_ge_floor
    from {{ ref('polymarket_wc2026_contract') }}
    where scope_name = 'wc2026'
),

stage_expectations as (
    select
        'round_of_32' as stage_key,
        knockout_min_volume_usd,
        round_of_32_min_raw_markets_ge_floor
    from contract
),

expected_stage_team_rows as (
    select
        e.stage_key,
        m.home_team as team_name
    from stage_expectations as e
    inner join {{ ref('international_results_wc2026_matches') }} as m
        on e.stage_key = m.stage_key

    union all

    select
        e.stage_key,
        m.away_team as team_name
    from stage_expectations as e
    inner join {{ ref('international_results_wc2026_matches') }} as m
        on e.stage_key = m.stage_key
),

expected_stage_teams as (
    select
        stage_key,
        team_name
    from expected_stage_team_rows
    group by 1, 2
),

public_stage_team_coverage as (
    select
        stage_key,
        canonical_team_name as team_name
    from {{ ref('polymarket_wc2026_knockout_markets') }}
    group by 1, 2
),

expected_missing_issues as (
    select
        e.stage_key,
        e.team_name,
        'sparse_stage_missing_team_coverage:'
        || e.stage_key
        || ':'
        || e.team_name as issue_key
    from expected_stage_teams as e
    left join public_stage_team_coverage as p
        on
            e.stage_key = p.stage_key
            and lower(e.team_name) = lower(p.team_name)
    where p.team_name is null
)

select
    e.issue_key,
    e.stage_key,
    e.team_name
from expected_missing_issues as e
left join {{ ref('polymarket_wc2026_knockout_data_quality') }} as d
    on
        e.issue_key = d.issue_key
        and d.severity = 'warn'
        and d.entity_type = 'team'
where d.issue_key is null
