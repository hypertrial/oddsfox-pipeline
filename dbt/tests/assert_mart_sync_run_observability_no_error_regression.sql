{{ config(severity = 'warn') }}

with latest_per_task as (
    select *
    from {{ ref('polymarket_wc2026_sync_run_observability') }}
    qualify row_number() over (
        partition by task_name
        order by recorded_at desc
    ) = 1
)

select *
from latest_per_task
where
    coalesce(error_tokens, 0) > 0
    or coalesce(permanent_error_tokens, 0) > 0
