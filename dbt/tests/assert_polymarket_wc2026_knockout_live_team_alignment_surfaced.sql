{{ config(
    severity = 'warn',
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_knockout_data_quality'},
            'asset_key': ['polymarket', 'wc2026', 'observability', 'knockout_data_quality']
        }
    }
) }}

with active_result_teams as (
    select team_name
    from {{ ref('international_results_wc2026_team_status') }}
    where tournament_status = 'active'
),

live_odds_teams as (
    select
        canonical_team_name as team_name,
        count(*) as live_market_rows
    from {{ ref('polymarket_wc2026_knockout_markets') }}
    where is_live_market
    group by 1
),

missing_live_odds as (
    select
        'active_team_missing_live_knockout_odds:' || r.team_name as issue_key,
        r.team_name,
        cast(null as bigint) as expected_count
    from active_result_teams as r
    left join live_odds_teams as l
        on lower(r.team_name) = lower(l.team_name)
    where l.team_name is null
),

eliminated_live_odds as (
    select
        'live_knockout_odds_for_eliminated_team:' || l.team_name as issue_key,
        l.team_name,
        l.live_market_rows as expected_count
    from live_odds_teams as l
    inner join {{ ref('international_results_wc2026_team_status') }} as r
        on lower(l.team_name) = lower(r.team_name)
    where r.tournament_status != 'active'
),

expected_issues as (
    select * from missing_live_odds
    union all
    select * from eliminated_live_odds
)

select
    e.issue_key,
    e.team_name,
    e.expected_count
from expected_issues as e
left join {{ ref('polymarket_wc2026_knockout_data_quality') }} as d
    on
        e.issue_key = d.issue_key
        and d.severity = 'warn'
        and d.entity_type = 'team'
where d.issue_key is null
