{{ config(materialized='table', tags=['polygon_settlement']) }}

select
    candidate.proposition_id,
    candidate.fifa_match_id,
    candidate.stage,
    candidate.group_name,
    candidate.home_team,
    candidate.away_team,
    candidate.proposition_type,
    candidate.yes_represents,
    candidate.no_represents,
    candidate.scheduled_kickoff_at_utc,
    candidate.analysis_window_start_at_utc,
    candidate.analysis_window_end_at_utc,
    candidate.settlement_minute_utc,
    candidate.settlement_minute_epoch,
    candidate.elapsed_window_minute,
    candidate.condition_id,
    candidate.yes_token_id,
    candidate.no_token_id,
    candidate.market_structure,
    candidate.exchange_address,
    candidate.manifest_sha256,
    candidate.manifest_version,
    candidate.yes_open_price as yes_open,
    candidate.yes_high_price as yes_high,
    candidate.yes_low_price as yes_low,
    candidate.yes_close_price as yes_close,
    candidate.yes_vwap,
    candidate.yes_normalized_fill_count,
    candidate.yes_derived_fill_count,
    candidate.yes_share_volume,
    candidate.yes_gross_collateral_volume,
    candidate.yes_first_settlement_at_utc,
    candidate.yes_last_settlement_at_utc,
    candidate.yes_observed,
    candidate.no_open_price as no_open,
    candidate.no_high_price as no_high,
    candidate.no_low_price as no_low,
    candidate.no_close_price as no_close,
    candidate.no_vwap,
    candidate.no_normalized_fill_count,
    candidate.no_derived_fill_count,
    candidate.no_share_volume,
    candidate.no_gross_collateral_volume,
    candidate.no_first_settlement_at_utc,
    candidate.no_last_settlement_at_utc,
    candidate.no_observed,
    candidate.minute_complete,
    candidate.minute_status
from
    {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
        as candidate
-- costguard: allow cross-join, the fail-closed publication gate is exactly one row.
cross join
    {{ ref('int_polymarket_wc2026_polygon_settlement_publication_gate') }}
        as gate
where gate.publication_ready
