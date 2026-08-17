{{ config(materialized='view') }}

with registry as (
    select * from {{ ref('stg_polymarket_soccer_match_result_registry') }}
),

events as (
    select * from {{ ref('stg_polymarket_soccer_event_latest') }}
),

exclusions as (
    select * from {{ source('polymarket_soccer_ops', 'match_result_registry_exclusions') }}
),

fetch_audit as (
    select * from {{ ref('stg_polymarket_soccer_match_minute_audit_latest') }}
),

token_status as (
    select * from {{ ref('polymarket_soccer_match_result_token_fetch_status') }}
),

observed as (
    select * from {{ ref('polymarket_soccer_match_result_minute_odds_observed') }}
),

dense as (
    select * from {{ ref('polymarket_soccer_match_result_minute_odds') }}
),

market_state as (
    select * from {{ ref('int_polymarket_soccer_match_result_market_state') }}
),

observed_state as (
    select distinct
        market_id,
        source_revision
    from {{ ref('int_polymarket_soccer_match_result_observed') }}
),

dense_state as (
    select distinct
        market_id,
        source_revision
    from {{ ref('int_polymarket_soccer_match_result_minute_odds') }}
),

catalog_metric as (
    select metrics_json
    from {{ source('polymarket_soccer_ops', 'sync_run_metrics') }}
    where task_name = 'event_catalog'
    order by recorded_at desc
    limit 1
),

last_full_success as (
    select finished_at
    from {{ source('polymarket_soccer_ops', 'pipeline_runs') }}
    where
        job_name = 'polymarket_soccer_full_pipeline'
        and status = 'success'
    order by finished_at desc
    limit 1
)

select
    (select count(*) from events) as catalog_events,
    coalesce(
        (
            select
                cast(
                    json_extract(metrics_json, '$.all_scan_partitions_complete')
                    as boolean
                )
            from catalog_metric
        ),
        false
    ) as catalog_converged,
    (select count(distinct event_id) from registry) as mapped_matches,
    (select count(*) from registry) as mapped_markets,
    (select count(*) from exclusions) as excluded_events,
    round(
        100.0 * (select count(distinct event_id) from registry)
        / nullif((select count(*) from events), 0),
        3
    ) as mapping_coverage_percent,
    (
        select count(distinct event_id) from registry
        where coverage_tier = 'guaranteed_tag_era'
    ) as guaranteed_tag_era_matches,
    (
        select count(distinct event_id) from registry
        where coverage_tier = 'pre_tag_best_effort'
    ) as pre_tag_best_effort_matches,
    (
        select count(distinct event_id) from registry
        where timing_status = 'explicit_finish'
    ) as explicit_finish_matches,
    (
        select count(distinct event_id) from registry
        where timing_status = 'inferred_closure'
    ) as inferred_closure_matches,
    (
        select count(distinct event_id) from registry
        where timing_status = 'inferred_five_hour_cap')
        as inferred_five_hour_cap_matches,
    round(
        100.0 * (
            select count(distinct event_id) from registry
            where timing_status = 'explicit_finish'
        ) / nullif((select count(distinct event_id) from registry), 0),
        3
    ) as explicit_finish_share_percent,
    round(
        100.0 * (
            select count(distinct event_id) from registry
            where timing_status = 'inferred_closure'
        ) / nullif((select count(distinct event_id) from registry), 0),
        3
    ) as inferred_closure_share_percent,
    round(
        100.0 * (
            select count(distinct event_id) from registry
            where timing_status = 'inferred_five_hour_cap'
        ) / nullif((select count(distinct event_id) from registry), 0),
        3
    ) as inferred_five_hour_cap_share_percent,
    (
        select count(distinct event_id) from registry
        where kickoff_source = 'market_game_start_time'
    ) as market_kickoff_matches,
    (
        select count(distinct event_id) from registry
        where kickoff_source = 'event_start_time'
    ) as event_kickoff_matches,
    (
        select count(*) from token_status
        where raw_published
    ) as published_tokens,
    (
        select count(*) from fetch_audit
        where fetch_status = 'empty'
    ) as empty_tokens,
    (
        select count(*) from fetch_audit
        where fetch_status in ('error', 'cancelled')
    ) as retry_tokens,
    (
        select count(*) from token_status
        where is_terminal_unavailable
    ) as terminal_unavailable_tokens,
    (select count(*) from observed) as observed_minutes,
    (select count(*) from dense) as dense_minutes,
    (
        select count(*) from market_state as expected_state
        where not exists (
            select 1 from observed_state as built_state
            where
                built_state.market_id = expected_state.market_id
                and built_state.source_revision = expected_state.source_revision
        )
    ) as dirty_observed_markets,
    (
        select count(*) from market_state as expected_state
        where not exists (
            select 1 from dense_state as built_state
            where
                built_state.market_id = expected_state.market_id
                and built_state.source_revision = expected_state.source_revision
        )
    ) as dirty_dense_markets,
    (
        select sum((date_diff('minute', window_start_at, window_end_at) + 1) * 3)
        from (select distinct
            event_id,
            window_start_at,
            window_end_at
        from registry) as event_windows
    ) as expected_dense_minutes,
    round(
        100.0 * (select count(*) from observed)
        / nullif((select count(*) from dense), 0),
        3
    ) as observed_minute_coverage_percent,
    round(
        100.0 * (select count(*) from dense)
        / nullif(
            (
                select
                    sum(
                        (date_diff('minute', window_start_at, window_end_at) + 1) * 3
                    )
                from (
                    select distinct
                        event_id,
                        window_start_at,
                        window_end_at
                    from registry
                ) as event_windows
            ),
            0
        ),
        3
    ) as dense_minute_coverage_percent,
    (
        select count(*) from dense
        where not is_observed and close_odds is not null
    ) as carried_minutes,
    (
        select max(retry_age_hours) from token_status
        where is_retry_backlog)
        as oldest_retry_hours,
    (select finished_at from last_full_success) as last_full_success_at,
    current_timestamp as measured_at
