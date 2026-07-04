select
    match_id,
    tournament,
    match_date
from {{ ref('international_results_wc2026_matches') }}
where
    tournament != 'FIFA World Cup'
    or match_date < date '2026-06-11'
    or match_date > date '2026-07-19'
