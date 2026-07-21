select blocking_issue_keys
from {{ ref('polymarket_wc2026_match_minute_odds_data_quality') }}
where blocking_issue_keys is not null
