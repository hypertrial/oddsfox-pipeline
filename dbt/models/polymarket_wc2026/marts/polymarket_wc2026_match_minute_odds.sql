{{ config(materialized='table', tags=['cross_domain']) }}

select candidate.*
from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }} as candidate
-- costguard: allow cross-join, the publication gate is exactly one row.
cross join {{ ref('int_polymarket_wc2026_match_minute_publication_gate') }} as gate
where gate.publication_ready
