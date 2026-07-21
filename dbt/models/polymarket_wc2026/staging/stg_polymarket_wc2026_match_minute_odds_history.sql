{{ config(tags=['cross_domain']) }}

select
    market_id,
    clobtokenid as clob_token_id,
    cast(timestamp as bigint) as odds_timestamp_epoch,
    cast(price as double) as price,
    cast(fidelity_minutes as integer) as fidelity_minutes,
    window_start_at as game_started_at_utc,
    window_end_at as game_finished_at_utc,
    ingested_at,
    to_timestamp(timestamp) at time zone 'UTC' as odds_timestamp_utc
from {{ source('polymarket_wc2026_raw', 'match_minute_odds_history') }}
