{{ config(alias='event_state_timing') }}

select
    match_id,
    event_id,
    event_type,
    event_minute,
    event_second,
    team,
    player,
    event_at,
    source_status,
    _snapshot_id as snapshot_id,
    _collected_at as collected_at
from {{ source('wc2026_canonical_raw', 'fotmob__events') }}
where _snapshot_id = {{ latest_wc2026_snapshot_id('fotmob') }}
