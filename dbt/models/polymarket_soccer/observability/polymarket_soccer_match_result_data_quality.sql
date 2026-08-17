{{ config(materialized='table') }}

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

catalog_metric as (
    select metrics_json
    from {{ source('polymarket_soccer_ops', 'sync_run_metrics') }}
    where task_name = 'event_catalog'
    order by recorded_at desc
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
    (
        select count(distinct event_id) from registry
        where kickoff_source = 'market_game_start_time'
    ) as market_kickoff_matches,
    (
        select count(distinct event_id) from registry
        where kickoff_source = 'event_start_time'
    ) as event_kickoff_matches,
    (
        select count(*) from fetch_audit
        where fetch_status = 'success' and raw_published
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
        select count(*) from dense
        where not is_observed and close_odds is not null
    ) as carried_minutes,
    current_timestamp as measured_at
