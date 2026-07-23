{{ config(tags=['polygon_settlement']) }}

select  -- noqa: ST06
    cast(scan_id as varchar) as scan_id,
    cast(manifest_version as varchar) as manifest_version,
    lower(cast(manifest_sha256 as varchar)) as manifest_sha256,
    cast(normalizer_version as varchar) as normalizer_version,
    cast(chain_id as integer) as chain_id,
    cast(provider_label as varchar) as provider_label,
    cast(provider_origin as varchar) as provider_origin,
    cast(finalized_head_number as bigint) as finalized_head_number,
    lower(cast(finalized_head_hash as varchar)) as finalized_head_hash,
    cast(target_ranges_json as varchar) as target_ranges_json,
    lower(cast(boundary_blocks_sha256 as varchar)) as boundary_blocks_sha256,
    lower(cast(status as varchar)) as status,
    cast(raw_published as boolean) as raw_published,
    lower(cast(verification_status as varchar)) as verification_status,
    cast(started_at as timestamp) as started_at,
    cast(finished_at as timestamp) as finished_at,
    cast(published_at as timestamp) as published_at,
    cast(error_type as varchar) as error_type,
    cast(error_message as varchar) as error_message
from {{ source('polymarket_wc2026_ops', 'polygon_settlement_scan_runs') }}
