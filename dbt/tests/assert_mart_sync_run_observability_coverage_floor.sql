{{ config(severity = 'warn') }}

with latest_sync as (
    select *
    from {{ ref('polymarket_wc2026_sync_run_observability') }}
    where task_name = 'sync_odds'
    order by recorded_at desc
    limit 1
)

select *
from latest_sync
where
    history_coverage_vs_market_tokens_ratio is not null
    and coalesce(market_tokens_distinct_tokens, 0) > 0
    and history_coverage_vs_market_tokens_ratio < 0.95
