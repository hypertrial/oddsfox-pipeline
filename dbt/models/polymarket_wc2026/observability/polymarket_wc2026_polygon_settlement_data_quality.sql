{{ config(materialized='table', tags=['polygon_settlement']) }}

with issue_summary as (
    select
        count(*) filter (where severity = 'warn') as warning_issue_count,
        count(*) filter (where severity = 'error') as error_issue_count
    from {{ ref('polymarket_wc2026_polygon_settlement_quality_issues') }}
),

quality as (
    select  -- noqa: ST06
        104 as expected_games,
        248 as expected_propositions,
        496 as expected_tokens,
        39120 as expected_minute_rows,
        seed_summary.*,
        scan_summary.*,
        raw_summary.*,
        minute_summary.*,
        issue_summary.*
    from {{ ref('int_polymarket_wc2026_polygon_settlement_seed_quality_summary') }} as seed_summary
    cross join {{ ref('int_polymarket_wc2026_polygon_settlement_scan_quality_summary') }} as scan_summary
    cross join {{ ref('int_polymarket_wc2026_polygon_settlement_raw_quality_summary') }} as raw_summary
    cross join {{ ref('int_polymarket_wc2026_polygon_settlement_minute_quality_summary') }} as minute_summary
    cross join issue_summary
),

with_blockers as (
    select
        *,
        nullif(concat_ws(
            ',',
            case
                when
                    seed_rows <> expected_propositions
                    or seed_propositions <> expected_propositions
                    or seed_games <> expected_games
                    or first_fifa_match_id <> 1
                    or last_fifa_match_id <> expected_games
                    or out_of_range_match_rows > 0
                    then 'seed_inventory'
            end,
            case
                when
                    group_games <> 72
                    or round_of_32_games <> 16
                    or round_of_16_games <> 8
                    or quarterfinal_games <> 4
                    or semifinal_games <> 2
                    or third_place_games <> 1
                    or final_games <> 1
                    or group_propositions <> 216
                    or knockout_propositions <> 32
                    then 'seed_stage_distribution'
            end,
            case when invalid_match_shapes > 0 then 'seed_proposition_shape' end,
            case
                when
                    seed_conditions <> expected_propositions
                    or seed_token_rows <> expected_tokens
                    or seed_tokens <> expected_tokens
                    then 'seed_unique_ids'
            end,
            case when invalid_window_rows > 0 then 'seed_windows' end,
            case
                when
                    missing_semantic_rows > 0
                    or invalid_market_id_rows > 0
                    or invalid_evidence_rows > 0
                    or prohibited_source_rows > 0
                    or manifest_version_count <> 1
                    or manifest_sha256_count <> 1
                    then 'seed_evidence'
            end,
            case when published_scan_count <> 1 then 'scan_missing' end,
            case
                when
                    scan_manifest_version <> manifest_version
                    or scan_manifest_sha256 <> manifest_sha256
                    then 'scan_manifest'
            end,
            case
                when
                    not scan_raw_published
                    or scan_status <> 'published'
                    or invalid_scan_rows > 0
                    then 'scan_integrity'
            end,
            case
                when
                    target_range_count = 0
                    or target_exchange_count <> 2
                    or invalid_target_ranges > 0
                    or target_partition_count <> target_range_count
                    or invalid_target_partitions > 0
                    or invalid_chunk_rows > 0
                    or unassigned_chunk_count > 0
                    or chunk_fill_mismatch_count > 0
                    then 'scan_chunks'
            end,
            case when raw_fill_rows = 0 then 'raw_empty' end,
            case
                when foreign_scan_fill_rows > 0 then 'raw_scan_mismatch'
            end,
            case when duplicate_fill_grains > 0 then 'raw_duplicates' end,
            case
                when invalid_normalization_pair_grains > 0
                    then 'raw_normalization_pairs'
            end,
            case
                when invalid_fill_mapping_rows > 0 then 'raw_mapping'
            end,
            case
                when invalid_fill_value_rows > 0 then 'raw_values'
            end,
            case when unmatched_fill_chunks > 0 then 'raw_chunk_coverage' end,
            case
                when
                    actual_minute_rows <> expected_minute_rows
                    or distinct_minute_grains <> expected_minute_rows
                    then 'minute_inventory'
            end,
            case when invalid_proposition_axes > 0 then 'minute_axis' end,
            case
                when invalid_candidate_state_rows > 0 then 'minute_values'
            end,
            case
                when candidate_fill_count <> raw_fill_rows
                    then 'aggregate_reconciliation'
            end,
            case when error_issue_count > 0 then 'quality_errors' end
        ), '') as blocking_issue_keys
    from quality
)

select  -- noqa: ST06
    *,
    blocking_issue_keys is null as publication_ready
from with_blockers
