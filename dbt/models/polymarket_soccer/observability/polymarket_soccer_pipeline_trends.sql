{{ config(materialized='view') }}

with step_metrics as (
    select
        dagster_run_id,
        max(cast(json_extract(metrics_json, '$.events') as bigint)) filter (
            where step_name = 'event_catalog'
        ) as catalog_events,
        max(cast(json_extract(metrics_json, '$.unique_markets') as bigint)) filter (
            where step_name = 'event_catalog'
        ) as catalog_markets,
        max(cast(json_extract(metrics_json, '$.matches') as bigint)) filter (
            where step_name = 'match_result_registry'
        ) as mapped_matches,
        max(cast(json_extract(metrics_json, '$.raw_published_tokens') as bigint)) filter (
            where step_name = 'match_minute_odds'
        ) as published_tokens,
        sum(cast(json_extract(metrics_json, '$.elapsed_seconds') as double))
            as duration_seconds,
        sum(cast(json_extract(metrics_json, '$.process_cpu_seconds') as double))
            as process_cpu_seconds,
        max(cast(json_extract(metrics_json, '$.peak_rss_bytes') as bigint))
            as peak_rss_bytes,
        min(cast(json_extract(metrics_json, '$.disk_free_bytes') as bigint))
            as minimum_disk_free_bytes,
        max(cast(json_extract(metrics_json, '$.warehouse_bytes') as bigint))
            as warehouse_bytes,
        max(cast(json_extract(metrics_json, '$.observed_minute_coverage_percent') as double)) filter (
            where step_name = 'dbt_build'
        ) as observed_minute_coverage_percent,
        max(cast(json_extract(metrics_json, '$.dense_minute_coverage_percent') as double)) filter (
            where step_name = 'dbt_build'
        ) as dense_minute_coverage_percent
    from {{ source('polymarket_soccer_ops', 'pipeline_step_runs') }}
    where status in ('success', 'partial')
    group by dagster_run_id
),

successful_runs as (
    select
        runs.dagster_run_id,
        runs.started_at,
        runs.finished_at,
        runs.status,
        step_metrics.catalog_events,
        step_metrics.catalog_markets,
        step_metrics.mapped_matches,
        step_metrics.published_tokens,
        step_metrics.duration_seconds,
        step_metrics.process_cpu_seconds,
        step_metrics.peak_rss_bytes,
        step_metrics.minimum_disk_free_bytes,
        step_metrics.warehouse_bytes,
        step_metrics.observed_minute_coverage_percent,
        step_metrics.dense_minute_coverage_percent,
        100.0 * step_metrics.mapped_matches
        / nullif(step_metrics.catalog_events, 0) as mapping_coverage_percent
    from {{ source('polymarket_soccer_ops', 'pipeline_runs') }} as runs
    inner join step_metrics on runs.dagster_run_id = step_metrics.dagster_run_id
    where
        runs.job_name = 'polymarket_soccer_full_pipeline'
        and runs.status = 'success'
)

select
    *,
    lag(catalog_events) over (order by finished_at) as previous_catalog_events,
    lag(catalog_markets) over (order by finished_at) as previous_catalog_markets,
    lag(mapping_coverage_percent) over (order by finished_at)
        as previous_mapping_coverage_percent,
    lag(duration_seconds) over (order by finished_at) as previous_duration_seconds,
    lag(process_cpu_seconds) over (order by finished_at) as previous_process_cpu_seconds,
    lag(peak_rss_bytes) over (order by finished_at) as previous_peak_rss_bytes,
    lag(warehouse_bytes) over (order by finished_at) as previous_warehouse_bytes,
    lag(observed_minute_coverage_percent) over (order by finished_at)
        as previous_observed_minute_coverage_percent,
    lag(dense_minute_coverage_percent) over (order by finished_at)
        as previous_dense_minute_coverage_percent
from successful_runs
