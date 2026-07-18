{{ config(alias='venue_markets') }}

select
    'polymarket' as venue,
    market_id,
    event_id,
    event_slug,
    question,
    outcomes,
    volume,
    is_active,
    is_closed,
    is_active as active,
    is_closed as closed,
    condition_id,
    clob_token_ids as token_ids,
    sports_market_type as market_type,
    game_start_time,
    end_date,
    scraped_at as observed_at,
    scope_name,
    is_resolved,
    winning_outcome,
    winning_clob_token_id
from {{ ref('int_polymarket_wc2026_markets') }}
