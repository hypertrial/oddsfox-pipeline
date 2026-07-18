{{ config(alias='team_ratings_history') }}

select
    snapshot_year,
    snapshot_scope,
    rank,
    team_code,
    team_name,
    rating,
    _snapshot_id as snapshot_id,
    _collected_at as collected_at
from {{ source('wc2026_canonical_raw', 'eloratings__team_ratings') }}
