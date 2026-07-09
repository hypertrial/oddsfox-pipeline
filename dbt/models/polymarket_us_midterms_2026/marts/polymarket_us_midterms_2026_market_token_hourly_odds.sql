select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.outcome_label,
    m.question,
    m.event_slug,
    m.is_active,
    m.is_closed,
    m.volume as market_volume_usd,
    h.odds_hour_utc,
    h.odds_hour_epoch,
    h.open_price,
    h.high_price,
    h.low_price,
    h.close_price,
    h.avg_price,
    h.observed_points,
    h.first_timestamp,
    h.first_observed_at,
    h.last_timestamp,
    h.last_observed_at
from {{ ref('int_polymarket_us_midterms_2026_market_tokens') }} as t
inner join {{ ref('int_polymarket_us_midterms_2026_markets') }} as m
    on t.market_id = m.market_id
inner join {{ ref('int_polymarket_us_midterms_2026_token_hourly_odds') }} as h
    on t.clob_token_id = h.clob_token_id
