{{ config(materialized='view') }}

with alerts as (
    select * from {{ ref('polymarket_soccer_pipeline_alerts') }}
),

latest_run as (
    select *
    from {{ source('polymarket_soccer_ops', 'pipeline_runs') }}
    where job_name = 'polymarket_soccer_full_pipeline'
    order by started_at desc
    limit 1
)

select
    (select latest_run.dagster_run_id from latest_run) as dagster_run_id,
    coalesce((select latest_run.status from latest_run), 'missing') as latest_run_status,
    (select latest_run.started_at from latest_run) as latest_run_started_at,
    (select latest_run.finished_at from latest_run) as latest_run_finished_at,
    count(*) filter (where alerts.severity = 'warning') as warning_count,
    count(*) filter (where alerts.severity = 'critical') as critical_count,
    case
        when count(*) filter (where alerts.severity = 'critical') > 0 then 'critical'
        when count(*) filter (where alerts.severity = 'warning') > 0 then 'warning'
        else 'healthy'
    end as health_status,
    current_timestamp as measured_at
from alerts
