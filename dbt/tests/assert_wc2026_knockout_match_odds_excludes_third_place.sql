select *
from {{ ref('wc2026_knockout_match_hourly_odds') }}
where fifa_match_id = 103
