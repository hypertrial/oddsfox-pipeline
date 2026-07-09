-- costguard: disable-file=SQLCOST007
-- costguard: disable-file=SQLCOST012
-- Window ordering defines OHLC open/close prices; SQLCOST012 is a false
-- positive on the close_rank expression after removing the one-row contract join.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['clob_token_id', 'odds_hour_epoch'],
        on_schema_change='fail',
        post_hook="
            delete from {{ this }}
            where odds_hour_utc < current_timestamp - (
                (
                    select hourly_window_days
                    from {{ ref('polymarket_us_midterms_2026_contract') }}
                    where scope_name = 'us_midterms_2026'
                ) * interval '1 day'
            )
        ",
    )
}}

with contract as (
    select hourly_window_days
    from {{ ref('polymarket_us_midterms_2026_contract') }}
    where scope_name = 'us_midterms_2026'
),

source_odds as (
    select
        o.clob_token_id,
        o.odds_timestamp,
        o.odds_timestamp_epoch,
        o.price,
        o.ingested_at,
        cast(epoch(date_trunc('hour', o.odds_timestamp)) as bigint) as odds_hour_epoch,
        date_trunc('hour', o.odds_timestamp) as odds_hour_utc
    from {{ ref('stg_polymarket_us_midterms_2026_odds') }} as o
    where
        o.price is not null
        and o.odds_timestamp is not null
        and o.odds_timestamp_epoch is not null
        and o.odds_timestamp >= current_timestamp - (
            (select contract.hourly_window_days from contract) * interval '1 day'
        )
),

dirty_hours as (
    select distinct
        clob_token_id,
        odds_hour_utc,
        odds_hour_epoch
    from source_odds
    {% if is_incremental() %}
        where
            ingested_at is null
            or ingested_at >= (
                select coalesce(max(latest_ingested_at), timestamp '1970-01-01')
                from {{ this }}
            ) - interval '2 hour'
    {% endif %}
),

hourly_source as (
    select
        source_odds.clob_token_id,
        source_odds.odds_timestamp,
        source_odds.odds_timestamp_epoch,
        source_odds.price,
        source_odds.ingested_at,
        source_odds.odds_hour_utc,
        source_odds.odds_hour_epoch
    from source_odds
    inner join dirty_hours
        on
            source_odds.clob_token_id = dirty_hours.clob_token_id
            and source_odds.odds_hour_epoch = dirty_hours.odds_hour_epoch
),

ranked as (
    select
        clob_token_id,
        odds_timestamp,
        odds_timestamp_epoch,
        price,
        ingested_at,
        odds_hour_utc,
        odds_hour_epoch,
        row_number() over (
            partition by clob_token_id, odds_hour_epoch
            order by odds_timestamp_epoch asc, price asc
        ) as open_rank,
        row_number() over (
            partition by clob_token_id, odds_hour_epoch
            order by odds_timestamp_epoch desc, price desc
        ) as close_rank
    from hourly_source
)

select
    clob_token_id,
    odds_hour_utc,
    odds_hour_epoch,
    max(case when open_rank = 1 then price end) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    max(case when close_rank = 1 then price end) as close_price,
    round(avg(price), 8) as avg_price,
    count(*) as observed_points,
    min(odds_timestamp_epoch) as first_timestamp,
    min(odds_timestamp) as first_observed_at,
    max(odds_timestamp_epoch) as last_timestamp,
    max(odds_timestamp) as last_observed_at,
    max(ingested_at) as latest_ingested_at
from ranked
group by
    clob_token_id,
    odds_hour_utc,
    odds_hour_epoch
