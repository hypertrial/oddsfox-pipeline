{{ config(alias='club_strength_history') }}

select
    club_key,
    club_name,
    country_code,
    elo,
    valid_from,
    valid_to,
    api_club_name,
    _snapshot_id as snapshot_id,
    _collected_at as collected_at
from {{ source('wc2026_canonical_raw', 'clubelo__club_ratings') }}
where _snapshot_id = {{ latest_wc2026_snapshot_id('clubelo') }}
