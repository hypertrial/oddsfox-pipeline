{{ config(alias='source_provenance') }}

select * from {{ source('oddsfox_reference', 'wc2026_source_provenance') }}

union all

select
    'polymarket' as source,
    run_id as snapshot_id,
    recorded_at as collected_at,
    cast(null as varchar) as collector_git_sha,
    cast(null as varchar) as collector_container_digest,
    md5(metrics_json) as manifest_sha256,
    recorded_at as loaded_at,
    'public_collector' as provenance_kind
from {{ source('polymarket_wc2026_ops', 'ingestion_run_events') }}

union all

select
    'kalshi' as source,
    run_id as snapshot_id,
    recorded_at as collected_at,
    cast(null as varchar) as collector_git_sha,
    cast(null as varchar) as collector_container_digest,
    md5(metrics_json) as manifest_sha256,
    recorded_at as loaded_at,
    'public_collector' as provenance_kind
from {{ source('kalshi_wc2026_ops', 'ingestion_run_events') }}
