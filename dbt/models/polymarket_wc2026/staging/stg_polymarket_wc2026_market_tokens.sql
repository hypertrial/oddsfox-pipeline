-- costguard: disable-file=SQLCOST012
select
    markets.market_id,
    cast(je.key as integer) as outcome_index,
    markets.scraped_at as updated_at,
    json_extract_string(je.value, '$') as clob_token_id
from {{ ref('stg_polymarket_wc2026_markets') }} as markets
cross join lateral json_each(markets.clob_token_ids) as je
where
    markets.clob_token_ids is not null
    and trim(markets.clob_token_ids) != ''
    and left(trim(markets.clob_token_ids), 1) = '['
