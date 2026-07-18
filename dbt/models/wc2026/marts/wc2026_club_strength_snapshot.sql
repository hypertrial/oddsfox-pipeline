{{ config(alias='club_strength_snapshot') }}

select
    snapshot_date,
    club_key,
    club_name,
    country_code,
    elo,
    valid_from,
    valid_to,
    rank,
    _snapshot_id as snapshot_id,
    _collected_at as collected_at
from {{ source('wc2026_canonical_raw', 'clubelo__club_ratings') }}
