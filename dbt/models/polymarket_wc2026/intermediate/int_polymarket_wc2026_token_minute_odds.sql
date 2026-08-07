{{ config(materialized='table', tags=['minute_odds']) }}

with match_token as (
    select
        clob_token_id,
        odds_minute_utc,
        odds_minute_epoch,
        open_price,
        high_price,
        low_price,
        close_price,
        average_price as avg_price,
        observed_points,
        first_observed_at,
        last_observed_at,
        'match' as minute_source
    from {{ ref('int_polymarket_wc2026_match_token_minute_odds') }}
),

futures_token as (
    select
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
),

combined as (
    select * from match_token
    union all
    select * from futures_token
)

select
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
    minute_source
from combined
qualify row_number() over (
    partition by clob_token_id, odds_minute_epoch
    order by
        case when minute_source = 'match' then 0 else 1 end,
        last_observed_at desc
) = 1
