{{ config(tags=['wc2026_logical_atlas']) }}

select
    event_id,
    market_id,
    source_ordinal,
    is_enclosing_event,
    event_logical_eligible,
    event_membership_status,
    event_ever_eligible,
    event_volume_unknown,
    fifa_match_id,
    fixture_mapping_basis,
    is_primary_qualifying_event
from {{ ref('int_polymarket_wc2026_logical_market_events') }}
