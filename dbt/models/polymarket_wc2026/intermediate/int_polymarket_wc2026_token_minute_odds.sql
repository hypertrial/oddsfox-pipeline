{{ config(materialized='view', tags=['minute_odds']) }}

-- Narrow primary-token fact for the unified minute mart. Match wins on any
-- accidental (clob_token_id, odds_minute_epoch) clash via anti-join (no global sort).
-- Both legs are pass-throughs over publish-time primary OHLC (no raw re-aggregate).
with match_primary as (
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
        last_observed_at,
        'match' as minute_source
    from {{ source('polymarket_wc2026_raw', 'match_primary_minute_ohlc') }}
),

futures_primary as (
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
        last_observed_at,
        'futures' as minute_source
    from {{ ref('int_polymarket_wc2026_futures_token_minute_odds') }}
)

select * from match_primary
union all
select futures_primary.*
from futures_primary
where not exists (
    select 1
    from match_primary as match_keys
    where
        match_keys.clob_token_id = futures_primary.clob_token_id
        and match_keys.odds_minute_epoch = futures_primary.odds_minute_epoch
)
