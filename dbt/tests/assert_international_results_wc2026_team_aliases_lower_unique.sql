select lower(market_team_name) as market_team_name_lower
from {{ ref('international_results_wc2026_team_aliases') }}
group by 1
having count(*) > 1
