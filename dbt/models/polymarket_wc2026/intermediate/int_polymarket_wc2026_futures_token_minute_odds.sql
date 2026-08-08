{{ config(materialized='view', tags=['minute_odds']) }}

-- Primary-token minute OHLC only. Raw futures history retains every CLOB token;
-- the unified mart publishes one primary outcome per market (Yes when present).
-- arg_min/arg_max replace twin row_number() sorts: raw PK (clobTokenId, timestamp)
-- makes same-second ties impossible, so epoch alone is a deterministic open/close key.
with windowed as (
    select
        primary_tokens.market_id,
        h.clob_token_id,
        h.odds_timestamp_epoch,
        h.odds_timestamp_utc,
        h.price,
        (h.odds_timestamp_epoch // 60) * 60 as odds_minute_epoch
    from {{ ref('stg_polymarket_wc2026_futures_minute_odds_history') }} as h
    inner join {{ ref('int_polymarket_wc2026_primary_market_token') }} as primary_tokens
        on h.clob_token_id = primary_tokens.clob_token_id
    inner join {{ ref('int_polymarket_wc2026_markets') }} as markets
        on primary_tokens.market_id = markets.market_id
    where
        h.odds_timestamp_utc >= h.window_started_at_utc
        and h.odds_timestamp_utc <= h.window_finished_at_utc
        and (
            markets.sports_market_type is null
            or lower(markets.sports_market_type) not in (
                'moneyline', 'soccer_team_to_advance'
            )
        )
)

select
    market_id,
    clob_token_id,
    to_timestamp(odds_minute_epoch) at time zone 'UTC' as odds_minute_utc,
    odds_minute_epoch,
    arg_min(price, odds_timestamp_epoch) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    arg_max(price, odds_timestamp_epoch) as close_price,
    round(avg(price), 8) as avg_price,
    count(*) as observed_points,
    min(odds_timestamp_utc) as first_observed_at,
    max(odds_timestamp_utc) as last_observed_at
from windowed
group by market_id, clob_token_id, odds_minute_epoch
