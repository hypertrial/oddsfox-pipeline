{{ config(materialized='table', tags=['cross_domain']) }}

with mapped_tokens as (
    select
        fifa_match_id,
        market_id,
        'yes' as token_side,
        yes_clob_token_id as clob_token_id,
        game_started_at_utc,
        game_finished_at_utc
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}

    union all

    select
        fifa_match_id,
        market_id,
        'no' as token_side,
        no_clob_token_id as clob_token_id,
        game_started_at_utc,
        game_finished_at_utc
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}
),

latest_fetch_run as (
    select fetch_run_id
    from {{ ref('stg_polymarket_wc2026_match_minute_fetch_audit') }}
    group by fetch_run_id
    order by max(fetch_finished_at) desc, fetch_run_id desc
    limit 1
),

latest_audit as (
    select fetch_audit.*
    from {{ ref('stg_polymarket_wc2026_match_minute_fetch_audit') }} as fetch_audit
    inner join latest_fetch_run as latest
        on fetch_audit.fetch_run_id = latest.fetch_run_id
),

history_with_gaps as (
    select
        clob_token_id,
        odds_timestamp_epoch,
        price,
        odds_timestamp_epoch - lag(odds_timestamp_epoch) over (
            partition by clob_token_id order by odds_timestamp_epoch
        ) as observation_gap_seconds
    from {{ ref('stg_polymarket_wc2026_match_minute_odds_history') }}
),

history_summary as (
    select
        clob_token_id,
        count(*) as raw_observation_count,
        count(distinct odds_timestamp_epoch // 60) as observed_minute_buckets,
        min(odds_timestamp_epoch) as first_observation_epoch,
        max(odds_timestamp_epoch) as last_observation_epoch,
        max(observation_gap_seconds) as max_observation_gap_seconds,
        count(distinct price) as distinct_price_count
    from history_with_gaps
    group by clob_token_id
)

select
    tokens.fifa_match_id,
    tokens.market_id,
    tokens.token_side,
    tokens.clob_token_id,
    fetch_audit.fetch_run_id as latest_fetch_run_id,
    fetch_audit.fetch_status as latest_fetch_status,
    fetch_audit.raw_published as latest_fetch_raw_published,
    fetch_audit.source_row_count as latest_source_row_count,
    fetch_audit.in_game_row_count as latest_in_game_row_count,
    fetch_audit.in_game_history_sha256 as latest_in_game_history_sha256,
    history.first_observation_epoch,
    history.last_observation_epoch,
    history.max_observation_gap_seconds,
    date_diff(
        'minute',
        date_trunc('minute', tokens.game_started_at_utc),
        date_trunc('minute', tokens.game_finished_at_utc)
    ) + 1 as expected_minute_buckets,
    coalesce(history.raw_observation_count, 0) as raw_observation_count,
    coalesce(history.observed_minute_buckets, 0) as observed_minute_buckets,
    history.first_observation_epoch - epoch(tokens.game_started_at_utc)
        as first_observation_offset_seconds,
    epoch(tokens.game_finished_at_utc) - history.last_observation_epoch
        as last_observation_offset_seconds,
    coalesce(history.distinct_price_count, 0) as distinct_price_count,
    coalesce(
        history.observed_minute_buckets::double
        / nullif(
            date_diff(
                'minute',
                date_trunc('minute', tokens.game_started_at_utc),
                date_trunc('minute', tokens.game_finished_at_utc)
            ) + 1,
            0
        ),
        0
    ) as observation_to_minute_ratio,
    coalesce(history.max_observation_gap_seconds > 120, false)
        as cadence_gap_warning,
    coalesce(
        history.first_observation_epoch - epoch(tokens.game_started_at_utc) > 120,
        false
    ) as first_boundary_offset_warning,
    coalesce(
        epoch(tokens.game_finished_at_utc) - history.last_observation_epoch > 120,
        false
    ) as last_boundary_offset_warning,
    coalesce(history.distinct_price_count = 1, false) as constant_price_warning
from mapped_tokens as tokens
left join latest_audit as fetch_audit
    on tokens.clob_token_id = fetch_audit.clob_token_id
left join history_summary as history
    on tokens.clob_token_id = history.clob_token_id
