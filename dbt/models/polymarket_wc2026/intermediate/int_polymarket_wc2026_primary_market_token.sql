select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.token_updated_at,
    t.outcome_label
from {{ ref('int_polymarket_wc2026_token_working_set') }} as t
inner join {{ ref('int_polymarket_wc2026_markets') }} as markets
    on t.market_id = markets.market_id
where lower(t.outcome_label) = 'yes'
qualify row_number() over (
    partition by t.market_id
    order by t.outcome_index asc, t.clob_token_id asc
) = 1
