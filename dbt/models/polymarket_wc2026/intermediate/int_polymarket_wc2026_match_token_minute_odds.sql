{{ config(materialized='table', tags=['cross_domain']) }}

with selected_tokens as (
    select
        market_id,
        yes_clob_token_id as clob_token_id,
        game_started_at_utc,
        game_finished_at_utc
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}
    union all
    select
        market_id,
        no_clob_token_id as clob_token_id,
        game_started_at_utc,
        game_finished_at_utc
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}
),

in_game as (
    select
        h.clob_token_id,
        h.odds_timestamp_epoch,
        h.odds_timestamp_utc,
        h.price,
        date_trunc('minute', h.odds_timestamp_utc) as odds_minute_utc
    from {{ ref('stg_polymarket_wc2026_match_minute_odds_history') }} as h
    inner join selected_tokens as t
        on
            h.market_id = t.market_id
            and h.clob_token_id = t.clob_token_id
    where
        h.odds_timestamp_utc >= t.game_started_at_utc
        and h.odds_timestamp_utc <= t.game_finished_at_utc
),

ranked as (
    select
        *,
        row_number() over (
            partition by clob_token_id, odds_minute_utc
            order by odds_timestamp_epoch, price
        ) as open_rank,
        row_number() over (
            partition by clob_token_id, odds_minute_utc
            order by odds_timestamp_epoch desc, price desc
        ) as close_rank
    from in_game
)

select
    clob_token_id,
    odds_minute_utc,
    cast(epoch(odds_minute_utc) as bigint) as odds_minute_epoch,
    max(case when open_rank = 1 then price end) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    max(case when close_rank = 1 then price end) as close_price,
    round(avg(price), 8) as average_price,
    count(*) as observed_points,
    min(odds_timestamp_utc) as first_observed_at,
    max(odds_timestamp_utc) as last_observed_at
from ranked
group by clob_token_id, odds_minute_utc
