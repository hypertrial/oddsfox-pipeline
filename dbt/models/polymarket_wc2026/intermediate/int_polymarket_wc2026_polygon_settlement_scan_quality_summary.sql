{{ config(materialized='table', tags=['polygon_settlement']) }}

with published_scans as (
    select *
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_runs') }}
    where status = 'published' and raw_published
),

published_scan_summary as (
    select count(*) as published_scan_count
    from published_scans
),

latest_published_scan as (
    select *
    from {{ ref('int_polymarket_wc2026_polygon_settlement_latest_published_scan') }}
),

scan_summary as (
    select
        max(scan_counts.published_scan_count) as published_scan_count,
        max(scan.scan_id) as scan_id,
        max(scan.status) as scan_status,
        coalesce(bool_and(scan.raw_published), false) as scan_raw_published,
        max(scan.manifest_version) as scan_manifest_version,
        max(scan.manifest_sha256) as scan_manifest_sha256,
        max(scan.normalizer_version) as scan_normalizer_version,
        max(scan.chain_id) as scan_chain_id,
        max(scan.finalized_head_number) as finalized_head_number,
        max(scan.finalized_head_hash) as finalized_head_hash,
        count(*) filter (
            where
            scan.scan_id is not null
            and (
                coalesce(scan.chain_id, -1) <> 137
                or coalesce(scan.normalizer_version, '')
                <> 'polygon-v2-settlement-v4'
                or not regexp_full_match(
                    coalesce(scan.manifest_sha256, ''), '[0-9a-f]{64}'
                )
                or not regexp_full_match(
                    coalesce(scan.finalized_head_hash, ''), '0x[0-9a-f]{64}'
                )
                or not regexp_full_match(
                    coalesce(scan.boundary_blocks_sha256, ''), '[0-9a-f]{64}'
                )
                or coalesce(scan.finalized_head_number, -1) < 0
                or nullif(scan.provider_label, '') is null
                or nullif(scan.provider_origin, '') is null
                or scan.target_ranges_json is null
                or scan.published_at is null
            )
        ) as invalid_scan_rows
    from published_scan_summary as scan_counts
    left join latest_published_scan as scan on true
),

target_ranges as (
    select  -- noqa: ST06
        scan.scan_id,
        lower(json_extract_string(target_range.value, '$.exchange_address'))
            as exchange_address,
        cast(json_extract(target_range.value, '$.from_block') as bigint)
            as from_block,
        cast(json_extract(target_range.value, '$.to_block') as bigint)
            as to_block,
        lower(json_extract_string(target_range.value, '$.from_block_hash'))
            as from_block_hash,
        lower(json_extract_string(target_range.value, '$.to_block_hash'))
            as to_block_hash
    from latest_published_scan as scan
    cross join json_each(scan.target_ranges_json) as target_range
),

ordered_target_ranges as (
    select
        *,
        lag(to_block)
            over (
                partition by exchange_address order by from_block, to_block
            ) as previous_to_block
    from target_ranges
),

target_range_summary as (
    select
        count(*) as target_range_count,
        count(distinct exchange_address) as target_exchange_count,
        count(*) filter (
            where
            exchange_address not in (
                '0xe111180000d2663c0091e4f400237545b87b996b',
                '0xe2222d279d744050d28e00520010520000310f59'
            )
            or from_block > to_block
            or to_block > (
                select max(scan.finalized_head_number)
                from latest_published_scan as scan
            )
            or previous_to_block is not null
            and from_block <= previous_to_block
            or not regexp_full_match(
                coalesce(from_block_hash, ''), '0x[0-9a-f]{64}'
            )
            or not regexp_full_match(
                coalesce(to_block_hash, ''), '0x[0-9a-f]{64}'
            )
        ) as invalid_target_ranges
    from ordered_target_ranges
),

target_partitions as (
    select * from target_ranges
),

ordered_partition_chunks as (
    select
        targets.*,
        chunks.from_block as chunk_from_block,
        chunks.to_block as chunk_to_block,
        chunks.from_block_hash as chunk_from_block_hash,
        chunks.to_block_hash as chunk_to_block_hash,
        lag(chunks.to_block) over (
            partition by
                targets.scan_id,
                targets.exchange_address,
                targets.from_block,
                targets.to_block
            order by chunks.from_block, chunks.to_block
        ) as previous_chunk_to_block
    from target_partitions as targets
    left join
        {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_chunks') }}
            as chunks
        on
            targets.scan_id = chunks.scan_id
            and targets.exchange_address = chunks.exchange_address
            and chunks.status = 'success'
            and targets.from_block <= chunks.from_block
            and targets.to_block >= chunks.to_block
),

partition_summary as (
    select
        scan_id,
        exchange_address,
        from_block,
        to_block,
        count(chunk_from_block) as chunk_count,
        min(chunk_from_block) as first_chunk_from_block,
        max(chunk_to_block) as last_chunk_to_block,
        max(
            case when chunk_from_block = from_block then chunk_from_block_hash end
        ) as first_chunk_hash,
        max(
            case when chunk_to_block = to_block then chunk_to_block_hash end
        ) as last_chunk_hash,
        count(*) filter (
            where
            previous_chunk_to_block is not null
            and chunk_from_block <> previous_chunk_to_block + 1
        ) as gap_or_overlap_count,
        max(from_block_hash) as target_from_block_hash,
        max(to_block_hash) as target_to_block_hash
    from ordered_partition_chunks
    group by scan_id, exchange_address, from_block, to_block
),

unassigned_chunks as (
    select count(*) as unassigned_chunk_count
    from
        {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_chunks') }}
            as chunks
    inner join latest_published_scan as scan on chunks.scan_id = scan.scan_id
    left join target_partitions as targets
        on
            chunks.scan_id = targets.scan_id
            and chunks.exchange_address = targets.exchange_address
            and chunks.from_block >= targets.from_block
            and chunks.to_block <= targets.to_block
    where chunks.status = 'success' and targets.scan_id is null
),

fills_by_chunk as (
    select
        scan_id,
        exchange_address,
        chunk_from_block,
        chunk_to_block,
        count(*) as fill_count
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_fills') }}
    group by scan_id, exchange_address, chunk_from_block, chunk_to_block
),

chunk_fill_reconciliation as (
    select count(*) as chunk_fill_mismatch_count
    from
        {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_chunks') }}
            as chunks
    inner join latest_published_scan as scan on chunks.scan_id = scan.scan_id
    left join fills_by_chunk as fills
        on
            chunks.scan_id = fills.scan_id
            and chunks.exchange_address = fills.exchange_address
            and chunks.from_block = fills.chunk_from_block
            and chunks.to_block = fills.chunk_to_block
    where
        chunks.status = 'success'
        and chunks.normalized_fill_count <> coalesce(fills.fill_count, 0)
),

invalid_chunks as (
    select count(*) as invalid_chunk_rows
    from
        {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_chunks') }}
            as chunks
    inner join latest_published_scan as scan on chunks.scan_id = scan.scan_id
    where
        chunks.status <> 'success'
        or chunks.from_block > chunks.to_block
        or not regexp_full_match(
            coalesce(chunks.from_block_hash, ''), '0x[0-9a-f]{64}'
        )
        or not regexp_full_match(
            coalesce(chunks.to_block_hash, ''), '0x[0-9a-f]{64}'
        )
        or not regexp_full_match(
            coalesce(chunks.scoped_event_sha256, ''), '[0-9a-f]{64}'
        )
        or chunks.event_count < 0
        or chunks.scoped_event_count < 0
        or chunks.scoped_event_count > chunks.event_count
        or chunks.normalized_fill_count < 0
        or chunks.duration_ms < 0
        or chunks.http_request_count < 0
        or chunks.log_rpc_call_count < 0
        or chunks.receipt_rpc_call_count < 0
        or chunks.header_rpc_call_count < 0
        or chunks.discovery_count < 0
        or chunks.eligible_discovery_count < 0
        or chunks.filtered_discovery_count < 0
        or chunks.receipt_transaction_count < 0
        or chunks.receipt_log_count < 0
        or chunks.retry_count < 0
        or chunks.adaptive_split_count < 0
        or chunks.eligible_discovery_count
        + chunks.filtered_discovery_count <> chunks.discovery_count
        or chunks.receipt_transaction_count
        > chunks.eligible_discovery_count
        or chunks.scoped_event_count > chunks.receipt_log_count
        or chunks.event_count
        <> chunks.discovery_count + chunks.receipt_log_count
),

chunk_summary as (
    select
        count(partitions.scan_id) as target_partition_count,
        count(*) filter (
            where
            partitions.scan_id is not null
            and (
                partitions.chunk_count = 0
                or partitions.first_chunk_from_block
                <> partitions.from_block
                or partitions.last_chunk_to_block <> partitions.to_block
                or partitions.gap_or_overlap_count > 0
                or coalesce(partitions.first_chunk_hash, '')
                <> coalesce(partitions.target_from_block_hash, '')
                or coalesce(partitions.last_chunk_hash, '')
                <> coalesce(partitions.target_to_block_hash, '')
            )
        ) as invalid_target_partitions,
        max(invalid.invalid_chunk_rows) as invalid_chunk_rows,
        max(unassigned.unassigned_chunk_count) as unassigned_chunk_count,
        max(reconciliation.chunk_fill_mismatch_count) as chunk_fill_mismatch_count,
        max(targets.target_range_count) as target_range_count,
        max(targets.target_exchange_count) as target_exchange_count,
        max(targets.invalid_target_ranges) as invalid_target_ranges
    from target_range_summary as targets
    cross join unassigned_chunks as unassigned
    cross join chunk_fill_reconciliation as reconciliation
    cross join invalid_chunks as invalid
    left join partition_summary as partitions on true
)

select
    scan_summary.*,
    chunk_summary.*
from scan_summary
cross join chunk_summary
