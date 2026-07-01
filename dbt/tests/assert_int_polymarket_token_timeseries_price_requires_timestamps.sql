-- Rows with a price must retain both timestamp fields.
select
    clob_token_id,
    price,
    odds_timestamp,
    odds_timestamp_epoch
from {{ ref('int_polymarket_token_timeseries') }}
where
    price is not null
    and (
        odds_timestamp is null
        or odds_timestamp_epoch is null
    )
