select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.updated_at as token_updated_at,
    m.question,
    m.event_slug,
    m.is_active,
    m.is_closed,
    m.volume as market_volume_usd,
    json_extract_string(m.outcomes, '$[' || t.outcome_index || ']') as outcome_label
from {{ ref('stg_polymarket_market_tokens') }} as t
inner join {{ ref('stg_polymarket_markets') }} as m
    on t.market_id = m.market_id
