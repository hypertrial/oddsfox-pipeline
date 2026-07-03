select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.question,
    t.outcome_label,
    t.event_slug,
    t.is_active,
    t.is_closed,
    t.market_volume_usd,
    o.odds_timestamp,
    o.odds_timestamp_epoch,
    o.price
from {{ ref('int_wc2026_polymarket_market_tokens') }} as t
inner join {{ ref('stg_wc2026_polymarket_odds') }} as o
    on t.clob_token_id = o.clob_token_id
where
    o.price is not null
    and o.odds_timestamp_epoch is not null
