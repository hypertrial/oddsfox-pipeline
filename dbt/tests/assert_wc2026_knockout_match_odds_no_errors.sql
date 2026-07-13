select *
from {{ ref('wc2026_knockout_match_odds_data_quality') }}
where severity = 'error'
