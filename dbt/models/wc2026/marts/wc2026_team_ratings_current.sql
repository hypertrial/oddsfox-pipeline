{{ config(alias='team_ratings_current') }}

select
    rank,
    team_code,
    team_name,
    rating,
    _snapshot_id as snapshot_id,
    _collected_at as collected_at
from {{ source('wc2026_canonical_raw', 'eloratings__team_ratings') }}
qualify row_number() over (
    partition by team_code
    order by _collected_at desc, snapshot_year desc nulls last
) = 1
