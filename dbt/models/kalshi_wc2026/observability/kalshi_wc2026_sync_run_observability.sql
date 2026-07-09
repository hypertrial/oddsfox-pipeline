select
    run_id,
    task_name,
    recorded_at,
    json_extract_string(metrics_json, '$.scope_name') as scope_name,
    try_cast(json_extract_string(metrics_json, '$.total_events') as bigint) as total_events,
    try_cast(json_extract_string(metrics_json, '$.total_markets') as bigint) as total_markets,
    try_cast(
        json_extract_string(metrics_json, '$.registry_summary.events_collected') as bigint
    ) as events_collected,
    try_cast(
        json_extract_string(metrics_json, '$.registry_summary.markets_collected') as bigint
    ) as markets_collected,
    try_cast(
        json_extract_string(metrics_json, '$.registry_summary.registry_rows') as bigint
    ) as registry_rows,
    try_cast(
        json_extract_string(metrics_json, '$.registry_summary.registry_upserted') as bigint
    ) as registry_upserted,
    try_cast(
        json_extract_string(metrics_json, '$.registry_summary.api_requests') as bigint
    ) as api_requests,
    try_cast(
        json_extract_string(metrics_json, '$.registry_summary.elapsed_seconds') as double
    ) as elapsed_seconds,
    try_cast(json_extract_string(metrics_json, '$.window_hours') as bigint) as window_hours,
    try_cast(json_extract_string(metrics_json, '$.markets_total') as bigint) as markets_total,
    try_cast(json_extract_string(metrics_json, '$.markets_synced') as bigint) as markets_synced,
    try_cast(json_extract_string(metrics_json, '$.empty_markets') as bigint) as empty_markets,
    try_cast(json_extract_string(metrics_json, '$.rows_written') as bigint) as rows_written,
    try_cast(json_extract_string(metrics_json, '$.force') as boolean) as is_force_sync
from {{ source('kalshi_wc2026_ops', 'pipeline_run_events') }}
where task_name in ('sync_kalshi_markets', 'sync_kalshi_candlesticks')
