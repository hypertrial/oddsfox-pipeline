{{ config(materialized='view', tags=['minute_odds']) }}

-- Pass-through over the landed primary-token minute OHLC parquet/table.
-- Raw futures history still retains every CLOB token; OHLC is produced at publish.
select
    market_id,
    clob_token_id,
    odds_minute_utc,
    odds_minute_epoch,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price,
    observed_points,
    first_observed_at,
    last_observed_at
from {{ source('polymarket_wc2026_raw', 'futures_primary_minute_ohlc') }}
