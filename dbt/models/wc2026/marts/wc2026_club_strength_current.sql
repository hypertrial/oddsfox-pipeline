{{ config(alias='club_strength_current') }}

select
    club_key,
    club_name,
    country_code,
    elo,
    rank,
    snapshot_date,
    _snapshot_id as snapshot_id,
    _collected_at as collected_at
from {{ source('wc2026_canonical_raw', 'clubelo__club_ratings') }}
where _snapshot_id = {{ latest_wc2026_snapshot_id('clubelo') }}
qualify row_number() over (
    partition by club_key
    order by snapshot_date desc nulls last, _collected_at desc
) = 1
