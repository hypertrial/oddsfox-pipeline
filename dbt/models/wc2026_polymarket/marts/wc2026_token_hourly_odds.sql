with hourly_odds as (
    select
        clob_token_id,
        odds_timestamp,
        odds_timestamp_epoch,
        price,
        date_trunc('hour', odds_timestamp) as odds_hour_utc
    from {{ ref('stg_wc2026_polymarket_odds') }}
    where
        price is not null
        and odds_timestamp is not null
        and odds_timestamp_epoch is not null
),

ranked as (
    select
        clob_token_id,
        odds_hour_utc,
        odds_timestamp,
        odds_timestamp_epoch,
        price,
        row_number() over (
            partition by clob_token_id, odds_hour_utc
            order by odds_timestamp_epoch asc, price asc
        ) as open_rank,
        row_number() over (
            partition by clob_token_id, odds_hour_utc
            order by odds_timestamp_epoch desc, price desc
        ) as close_rank
    from hourly_odds
),

aggregated as (
    select
        clob_token_id,
        odds_hour_utc,
        cast(epoch(odds_hour_utc) as bigint) as odds_hour_epoch,
        max(case when open_rank = 1 then price end) as open_price,
        max(price) as high_price,
        min(price) as low_price,
        max(case when close_rank = 1 then price end) as close_price,
        round(avg(price), 8) as avg_price,
        count(*) as observed_points,
        min(odds_timestamp_epoch) as first_timestamp,
        min(odds_timestamp) as first_observed_at,
        max(odds_timestamp_epoch) as last_timestamp,
        max(odds_timestamp) as last_observed_at
    from ranked
    group by 1, 2
)

select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.question,
    t.outcome_label,
    t.event_slug,
    t.is_active,
    t.is_closed,
    t.market_volume_usd,
    a.odds_hour_utc,
    a.odds_hour_epoch,
    a.open_price,
    a.high_price,
    a.low_price,
    a.close_price,
    a.avg_price,
    a.observed_points,
    a.first_timestamp,
    a.first_observed_at,
    a.last_timestamp,
    a.last_observed_at
from {{ ref('int_wc2026_polymarket_market_tokens') }} as t
inner join aggregated as a
    on t.clob_token_id = a.clob_token_id
