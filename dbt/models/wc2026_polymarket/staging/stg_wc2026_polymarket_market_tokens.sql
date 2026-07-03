-- costguard: disable-file=SQLCOST012
select
    mt.market_id,
    cast(je.key as integer) as outcome_index,
    mt.updated_at,
    json_extract_string(je.value, '$') as clob_token_id
from {{ source('wc2026_polymarket_raw', 'market_tokens') }} as mt
-- costguard: allow cross-join
cross join lateral json_each(mt.clobtokenids) as je
