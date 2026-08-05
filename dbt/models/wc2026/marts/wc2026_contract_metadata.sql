{{ config(alias='contract_metadata') }}

select
    'wc2026' as contract_name,
    'wc2026.v1' as contract_version,
    md5(
        'wc2026.v1|fixtures|results|team_identities|player_features|'
        || 'squad_player_features|team_ratings|team_ratings_pre_match|'
        || 'club_strength|base_camp_venues|travel_features|venue_markets|'
        || 'price_liquidity|event_state_timing|international_matches|'
        || 'third_place_slot_assignments|source_provenance'
    ) as contract_fingerprint,
    '{{ var("pipeline_git_sha", "unknown") }}' as pipeline_git_sha,
    string_agg(source || ':' || snapshot_id, ',' order by source, snapshot_id)
        as input_snapshot_ids,
    current_timestamp as built_at
from {{ ref('wc2026_source_provenance') }}
