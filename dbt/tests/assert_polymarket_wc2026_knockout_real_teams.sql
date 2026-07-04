select
    market_id,
    clob_token_id,
    team_name,
    canonical_team_name
from {{ ref('polymarket_wc2026_knockout_market_tokens') }}
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
