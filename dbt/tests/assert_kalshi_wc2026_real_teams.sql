{{
    config(
        meta={
            'dagster': {
                'ref': {'name': 'kalshi_wc2026_stage_markets'},
                'asset_key': ['kalshi', 'wc2026', 'marts', 'stage_markets']
            }
        }
    )
}}

select
    market_ticker,
    team_name,
    canonical_team_name
from {{ ref('kalshi_wc2026_stage_markets') }}
where
    canonical_team_name is null
    or lower(team_name) in (
        'italy',
        'europe',
        'uefa',
        'africa',
        'caf',
        'north america',
        'concacaf',
        'south america',
        'conmebol'
    )
    or lower(canonical_team_name) in (
        'italy',
        'europe',
        'uefa',
        'africa',
        'caf',
        'north america',
        'concacaf',
        'south america',
        'conmebol'
    )

union all

select
    market_ticker,
    team_name,
    canonical_team_name
from {{ ref('kalshi_wc2026_group_winner_markets') }}
where
    canonical_team_name is null
    or lower(team_name) in (
        'italy',
        'europe',
        'uefa',
        'africa',
        'caf',
        'north america',
        'concacaf',
        'south america',
        'conmebol'
    )
    or lower(canonical_team_name) in (
        'italy',
        'europe',
        'uefa',
        'africa',
        'caf',
        'north america',
        'concacaf',
        'south america',
        'conmebol'
    )
