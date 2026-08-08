{{ config(materialized='table', tags=['minute_odds']) }}

-- Narrow primary-token fact for the unified minute mart. Match wins on any
-- accidental (clob_token_id, odds_minute_epoch) clash via anti-join (no global sort).
with match_primary as (
    select
        tokens.market_id,
        odds.clob_token_id,
        odds.odds_minute_utc,
        odds.odds_minute_epoch,
        odds.open_price,
        odds.high_price,
        odds.low_price,
        odds.close_price,
        odds.average_price as avg_price,
        odds.observed_points,
        odds.first_observed_at,
        odds.last_observed_at,
        'match' as minute_source
    from {{ ref('int_polymarket_wc2026_match_token_minute_odds') }} as odds
    inner join {{ ref('int_polymarket_wc2026_primary_market_token') }} as tokens
        on odds.clob_token_id = tokens.clob_token_id
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
