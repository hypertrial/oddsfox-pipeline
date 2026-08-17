{{ config(materialized='view') }}

with latest_run as (
    select *
    from {{ source('polymarket_soccer_ops', 'pipeline_runs') }}
    where job_name = 'polymarket_soccer_full_pipeline'
    order by started_at desc
    limit 1
),

latest_success as (
    select *
    from {{ source('polymarket_soccer_ops', 'pipeline_runs') }}
    where
        job_name = 'polymarket_soccer_full_pipeline'
        and status = 'success'
    order by finished_at desc
    limit 1
),

latest_trend as (
    select * from {{ ref('polymarket_soccer_pipeline_trends') }}
    order by finished_at desc
    limit 1
),

retry_health as (
    select
        count(*) filter (where is_retry_backlog) as retry_tokens,
        max(retry_age_hours) filter (where is_retry_backlog) as oldest_retry_hours
    from {{ ref('polymarket_soccer_match_result_token_fetch_status') }}
),

stale_runs as (
    select count(*) as stale_count
    from {{ source('polymarket_soccer_ops', 'pipeline_runs') }}
    where
        status = 'running'
        and date_diff('hour', heartbeat_at, current_timestamp)
        > {{ env_var('POLYMARKET_SOCCER_MONITOR_STALE_RUN_HOURS', '6') | int }}
),

failed_attempts as (
    select count(*) as attempts
    from {{ source('polymarket_soccer_ops', 'pipeline_step_runs') }}
    where
        status in ('failed', 'interrupted')
        and started_at >= current_timestamp - interval '72 hours'
),

alerts as (
    select
        'missing_or_failed_full_run' as alert_code,
        'critical' as severity,
        'full_pipeline' as subject,
        coalesce((select status from latest_run), 'missing') as measured_value,
        'success_or_partial' as threshold_value,
        'Run the soccer full pipeline and inspect the failed ledger step.' as message
    where coalesce((select status from latest_run), 'missing') in ('missing', 'failed', 'interrupted')

    union all

    select
        'partial_full_run' as alert_code,
        'warning' as severity,
        'full_pipeline' as subject,
        'partial' as measured_value,
        'success' as threshold_value,
        'Inspect token failures; successful token publication remains available.' as message
    where (select status from latest_run) = 'partial'

    union all

    select
        'stale_running_run' as alert_code,
        'critical' as severity,
        'pipeline_runs' as subject,
        cast(stale_count as text) as measured_value,
        '{{ env_var("POLYMARKET_SOCCER_MONITOR_STALE_RUN_HOURS", "6") }}' as threshold_value,
        'Inspect or terminate the orphaned Dagster run before retrying.' as message
    from stale_runs
    where stale_count > 0

    union all

    select
        'stale_full_run' as alert_code,
        'critical' as severity,
        'full_pipeline' as subject,
        cast(date_diff(
            'hour',
            coalesce(
                (select finished_at from latest_success),
                (select coalesce(finished_at, started_at) from latest_run)
            ),
            current_timestamp
        ) as text) as measured_value,
        '{{ env_var("POLYMARKET_SOCCER_MONITOR_MAX_SUCCESS_AGE_HOURS", "30") }}' as threshold_value,
        'Run the soccer full pipeline; its latest completion is stale.' as message
    where
        coalesce(
            (select finished_at from latest_success),
            (select coalesce(finished_at, started_at) from latest_run)
        ) is not null
        and date_diff(
            'hour',
            coalesce(
                (select finished_at from latest_success),
                (select coalesce(finished_at, started_at) from latest_run)
            ),
            current_timestamp
        )
        > {{ env_var('POLYMARKET_SOCCER_MONITOR_MAX_SUCCESS_AGE_HOURS', '30') | int }}

    union all

    select
        'aged_retry_backlog' as alert_code,
        case
            when oldest_retry_hours > {{ env_var('POLYMARKET_SOCCER_MONITOR_RETRY_CRITICAL_HOURS', '72') | int }}
                then 'critical'
            else 'warning'
        end as severity,
        'clob_tokens' as subject,
        cast(oldest_retry_hours as text) as measured_value,
        '{{ env_var("POLYMARKET_SOCCER_MONITOR_RETRY_WARN_HOURS", "24") }}' as threshold_value,
        'Retry the due CLOB tokens; successful tokens remain published.' as message
    from retry_health
    where oldest_retry_hours > {{ env_var('POLYMARKET_SOCCER_MONITOR_RETRY_WARN_HOURS', '24') | int }}

    union all

    select
        'catalog_event_drop' as alert_code,
        'warning' as severity,
        'catalog_events' as subject,
        cast(catalog_events as text) as measured_value,
        cast(previous_catalog_events as text) as threshold_value,
        'Review Gamma catalog convergence and exact-tag membership.' as message
    from latest_trend
    where
        previous_catalog_events > 0
        and catalog_events < previous_catalog_events
        * (1 - {{ env_var('POLYMARKET_SOCCER_MONITOR_COUNT_DROP_FRACTION', '0.10') | float }})

    union all

    select
        'catalog_market_drop' as alert_code,
        'warning' as severity,
        'catalog_markets' as subject,
        cast(catalog_markets as text) as measured_value,
        cast(previous_catalog_markets as text) as threshold_value,
        'Review Gamma nested-market membership and catalog convergence.' as message
    from latest_trend
    where
        previous_catalog_markets > 0
        and catalog_markets < previous_catalog_markets
        * (1 - {{ env_var('POLYMARKET_SOCCER_MONITOR_COUNT_DROP_FRACTION', '0.10') | float }})

    union all

    select
        'mapping_coverage_drop' as alert_code,
        'warning' as severity,
        'mapping_coverage_percent' as subject,
        cast(mapping_coverage_percent as text) as measured_value,
        cast(previous_mapping_coverage_percent as text) as threshold_value,
        'Review new registry exclusion reasons; mapping remains fail closed.' as message
    from latest_trend
    where
        mapping_coverage_percent
        < previous_mapping_coverage_percent
        - {{ env_var('POLYMARKET_SOCCER_MONITOR_COVERAGE_DROP_POINTS', '5.0') | float }}

    union all

    select
        'observed_coverage_drop' as alert_code,
        'warning' as severity,
        'observed_minute_coverage_percent' as subject,
        cast(observed_minute_coverage_percent as text) as measured_value,
        cast(previous_observed_minute_coverage_percent as text) as threshold_value,
        'Inspect current-window CLOB publication and the retry backlog.' as message
    from latest_trend
    where
        observed_minute_coverage_percent
        < previous_observed_minute_coverage_percent
        - {{ env_var('POLYMARKET_SOCCER_MONITOR_COVERAGE_DROP_POINTS', '5.0') | float }}

    union all

    select
        'duration_regression' as alert_code,
        'warning' as severity,
        'full_pipeline' as subject,
        cast(duration_seconds as text) as measured_value,
        cast(previous_duration_seconds as text) as threshold_value,
        'Inspect step durations and upstream request rates before the next run.' as message
    from latest_trend
    where
        duration_seconds > previous_duration_seconds
        * {{ env_var('POLYMARKET_SOCCER_MONITOR_REGRESSION_RATIO', '2.0') | float }}
        and duration_seconds - previous_duration_seconds >= 300

    union all

    select
        'cpu_regression' as alert_code,
        'warning' as severity,
        'full_pipeline' as subject,
        cast(process_cpu_seconds as text) as measured_value,
        cast(previous_process_cpu_seconds as text) as threshold_value,
        'Inspect catalog normalization and dbt query plans for excess CPU work.' as message
    from latest_trend
    where
        process_cpu_seconds > previous_process_cpu_seconds
        * {{ env_var('POLYMARKET_SOCCER_MONITOR_REGRESSION_RATIO', '2.0') | float }}
        and process_cpu_seconds - previous_process_cpu_seconds >= 300

    union all

    select
        'rss_regression' as alert_code,
        'warning' as severity,
        'full_pipeline' as subject,
        cast(peak_rss_bytes as text) as measured_value,
        cast(previous_peak_rss_bytes as text) as threshold_value,
        'Inspect catalog batch size and dbt materializations for memory growth.' as message
    from latest_trend
    where
        peak_rss_bytes > previous_peak_rss_bytes
        * {{ env_var('POLYMARKET_SOCCER_MONITOR_REGRESSION_RATIO', '2.0') | float }}
        and peak_rss_bytes - previous_peak_rss_bytes >= 536870912

    union all

    select
        'low_free_disk' as alert_code,
        case
            when
                minimum_disk_free_bytes
                < {{ env_var('POLYMARKET_SOCCER_MONITOR_DISK_CRITICAL_GIB', '2') | float }} * 1073741824
                then 'critical'
            else 'warning'
        end as severity,
        'runtime_root' as subject,
        cast(minimum_disk_free_bytes as text) as measured_value,
        cast(case
            when
                minimum_disk_free_bytes
                < {{ env_var('POLYMARKET_SOCCER_MONITOR_DISK_CRITICAL_GIB', '2') | float }} * 1073741824
                then {{ env_var('POLYMARKET_SOCCER_MONITOR_DISK_CRITICAL_GIB', '2') | float }} * 1073741824
            else {{ env_var('POLYMARKET_SOCCER_MONITOR_DISK_WARN_GIB', '10') | float }} * 1073741824
        end as text) as threshold_value,
        'Free local disk space before the next catalog or minute publication.' as message
    from latest_trend
    where
        minimum_disk_free_bytes
        < {{ env_var('POLYMARKET_SOCCER_MONITOR_DISK_WARN_GIB', '10') | float }} * 1073741824

    union all

    select
        'warehouse_storage_regression' as alert_code,
        'warning' as severity,
        'warehouse_bytes' as subject,
        cast(warehouse_bytes as text) as measured_value,
        cast(previous_warehouse_bytes as text) as threshold_value,
        'Inspect retained snapshots and table growth before the next backfill.' as message
    from latest_trend
    where
        warehouse_bytes > previous_warehouse_bytes
        * {{ env_var('POLYMARKET_SOCCER_MONITOR_REGRESSION_RATIO', '2.0') | float }}
        and warehouse_bytes - previous_warehouse_bytes >= 1073741824

    union all

    select
        'repeated_step_failures' as alert_code,
        'warning' as severity,
        'pipeline_steps' as subject,
        cast(attempts as text) as measured_value,
        '2' as threshold_value,
        'Inspect recent failed step attempts and resolve the recurring cause.' as message
    from failed_attempts
    where attempts >= 2
)

select
    alerts.alert_code,
    alerts.severity,
    alerts.subject,
    alerts.measured_value,
    alerts.threshold_value,
    alerts.message,
    (select latest_run.dagster_run_id from latest_run) as dagster_run_id,
    coalesce(
        history.first_observed_at,
        (select latest_run.started_at from latest_run),
        current_timestamp
    ) as first_observed_at,
    current_timestamp as last_observed_at
from alerts
left join {{ source('polymarket_soccer_ops', 'pipeline_alert_history') }} as history
    on alerts.alert_code = history.alert_code and alerts.subject = history.subject
