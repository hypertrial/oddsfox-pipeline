{{ config(alias='source_provenance') }}

select
    source,
    snapshot_id,
    collected_at,
    collector_git_sha,
    collector_container_digest,
    manifest_sha256,
    loaded_at,
    'canonical_snapshot' as provenance_kind
from {{ source('wc2026_snapshot_ops', 'raw_snapshot_ledger') }}

union all

select
    'international_results' as source,
    md5(string_agg(source_row_hash, '' order by match_id)) as snapshot_id,
    max(source_loaded_at) as collected_at,
    cast(null as varchar) as collector_git_sha,
    cast(null as varchar) as collector_container_digest,
    md5(string_agg(source_row_hash, '' order by match_id)) as manifest_sha256,
    max(source_loaded_at) as loaded_at,
    'public_collector' as provenance_kind
from {{ source('international_results_wc2026_raw', 'historical_matches') }}
having count(*) > 0

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
from {{ source('polymarket_wc2026_ops', 'pipeline_run_events') }}

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
from {{ source('kalshi_wc2026_ops', 'pipeline_run_events') }}
