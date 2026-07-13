-- costguard: disable-file=SQLCOST012
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['clob_token_id', 'odds_hour_epoch'],
        on_schema_change='fail',
    )
}}

with eligible_tokens as (
    select clob_token_id
    from {{ ref('int_polymarket_wc2026_match_advance_tokens') }}
    where not is_ambiguous_mapping
),

source_odds as (
    select
        o.clob_token_id,
        o.odds_timestamp,
        o.odds_timestamp_epoch,
        o.price,
        o.ingested_at,
        cast(floor(o.odds_timestamp_epoch / 3600) * 3600 as bigint)
            as odds_hour_epoch,
        to_timestamp(floor(o.odds_timestamp_epoch / 3600) * 3600)
        at time zone 'UTC' as odds_hour_utc
    from {{ ref('stg_polymarket_wc2026_odds') }} as o
    inner join eligible_tokens as t
        on o.clob_token_id = t.clob_token_id
    where
        o.price is not null
        and o.odds_timestamp is not null
        and o.odds_timestamp_epoch is not null
),

dirty_hours as (
    select distinct
        o.clob_token_id,
        o.odds_hour_utc,
        o.odds_hour_epoch
    from source_odds as o
    {% if is_incremental() %}
        where
            o.ingested_at is null
            or o.ingested_at >= (
                select coalesce(max(latest_ingested_at), timestamp '1970-01-01')
                from {{ this }}
            ) - interval '2 hour'
            or not exists (
                select 1
                from {{ this }} as existing
                where existing.clob_token_id = o.clob_token_id
            )
    {% endif %}
),

ranked as (
    select
        o.clob_token_id,
        o.odds_timestamp,
        o.odds_timestamp_epoch,
        o.price,
        o.ingested_at,
        o.odds_hour_epoch,
        o.odds_hour_utc,
        row_number() over (
            partition by o.clob_token_id, o.odds_hour_epoch
            order by o.odds_timestamp_epoch, o.price
        ) as open_rank,
        row_number() over (
            partition by o.clob_token_id, o.odds_hour_epoch
            order by o.odds_timestamp_epoch desc, o.price desc
        ) as close_rank
    from source_odds as o
    inner join dirty_hours as d
        on
            o.clob_token_id = d.clob_token_id
            and o.odds_hour_epoch = d.odds_hour_epoch
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
    max(odds_timestamp_epoch) as last_timestamp,
    max(ingested_at) as latest_ingested_at
from ranked
group by clob_token_id, odds_hour_utc, odds_hour_epoch
