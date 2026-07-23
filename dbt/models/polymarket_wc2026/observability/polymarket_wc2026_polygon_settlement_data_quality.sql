{{ config(materialized='table', tags=['polygon_settlement']) }}

with seed as (
    select *
    from {{ ref('int_polymarket_wc2026_polygon_settlement_market_universe') }}
),

seed_tokens as (
    select yes_token_id as token_id from seed
    union all
    select no_token_id as token_id from seed
),

match_shape as (
    select
        fifa_match_id,
        max(stage) as stage,  -- noqa: RF04
        count(*) as proposition_count,
        count(distinct proposition_type) as proposition_type_count,
        count(distinct stage) as stage_count,
        count(distinct group_name) as group_count,
        count(distinct home_team) as home_team_count,
        count(distinct away_team) as away_team_count,
        count(distinct scheduled_kickoff_at_utc) as kickoff_count,
        count(distinct analysis_window_start_at_utc) as window_start_count,
        count(distinct analysis_window_end_at_utc) as window_end_count,
        count(*) filter (
            where
            stage = 'group_stage'
            and proposition_type in ('home_win', 'draw', 'away_win')
        ) as valid_group_proposition_count,
        count(*) filter (
            where
            fifa_match_id between 73 and 102
            and proposition_type = 'home_advances'
        ) as valid_advance_proposition_count,
        count(*) filter (
            where
            fifa_match_id = 103
            and proposition_type = 'home_win_third_place'
        ) as valid_third_place_proposition_count,
        count(*) filter (
            where
            fifa_match_id = 104
            and proposition_type = 'home_wins_final'
        ) as valid_final_proposition_count
    from seed
    group by fifa_match_id
),

seed_summary as (
    select
        count(*) as seed_rows,
        count(distinct proposition_id) as seed_propositions,
        count(distinct condition_id) as seed_conditions,
        count(distinct fifa_match_id) as seed_games,
        min(fifa_match_id) as first_fifa_match_id,
        max(fifa_match_id) as last_fifa_match_id,
        count(*) filter (where fifa_match_id not between 1 and 104)
            as out_of_range_match_rows,
        count(*) filter (where stage = 'group_stage') as group_propositions,
        count(*) filter (where stage <> 'group_stage') as knockout_propositions,
        count(distinct fifa_match_id) filter (where stage = 'group_stage')
            as group_games,
        count(distinct fifa_match_id) filter (where stage = 'round_of_32')
            as round_of_32_games,
        count(distinct fifa_match_id) filter (where stage = 'round_of_16')
            as round_of_16_games,
        count(distinct fifa_match_id) filter (where stage = 'quarterfinal')
            as quarterfinal_games,
        count(distinct fifa_match_id) filter (where stage = 'semifinal')
            as semifinal_games,
        count(distinct fifa_match_id) filter (where stage = 'third_place')
            as third_place_games,
        count(distinct fifa_match_id) filter (where stage = 'final')
            as final_games,
        count(distinct manifest_version) as manifest_version_count,
        max(manifest_version) as manifest_version,
        count(distinct manifest_sha256) as manifest_sha256_count,
        max(manifest_sha256) as manifest_sha256,
        count(*) filter (
            where
            proposition_id is null
            or home_team is null
            or away_team is null
            or home_team = away_team
            or yes_represents is null
            or no_represents is null
            or scheduled_kickoff_at_utc is null
            or analysis_window_start_at_utc is null
            or analysis_window_end_at_utc is null
            or (stage = 'group_stage' and group_name is null)
            or reviewed_at_utc is null
            or not regexp_full_match(
                coalesce(manifest_version, ''), '[0-9]+\.[0-9]+\.[0-9]+'
            )
            or not regexp_full_match(
                coalesce(manifest_sha256, ''), '[0-9a-f]{64}'
            )
        ) as missing_semantic_rows,
        count(*) filter (
            where
            analysis_window_start_at_utc <> scheduled_kickoff_at_utc
            or date_trunc('minute', scheduled_kickoff_at_utc)
            <> scheduled_kickoff_at_utc
            or date_trunc('minute', analysis_window_start_at_utc)
            <> analysis_window_start_at_utc
            or date_trunc('minute', analysis_window_end_at_utc)
            <> analysis_window_end_at_utc
            or (
                stage = 'group_stage'
                and window_minutes <> 150
            )
            or (
                stage <> 'group_stage'
                and window_minutes <> 210
            )
        ) as invalid_window_rows,
        count(*) filter (
            where
            not regexp_full_match(
                coalesce(condition_id, ''), '0x[0-9a-f]{64}'
            )
            or not regexp_full_match(
                coalesce(proposition_id, ''), '[a-z0-9][a-z0-9_-]*'
            )
            or not regexp_full_match(
                coalesce(yes_token_id, ''), '(0|[1-9][0-9]{0,77})'
            )
            or not regexp_full_match(
                coalesce(no_token_id, ''), '(0|[1-9][0-9]{0,77})'
            )
            or coalesce(market_structure, '') not in ('standard', 'neg_risk')
            or coalesce(exchange_address, '') not in (
                '0xe111180000d2663c0091e4f400237545b87b996b',
                '0xe2222d279d744050d28e00520010520000310f59'
            )
            or (
                market_structure = 'standard'
                and exchange_address
                <> '0xe111180000d2663c0091e4f400237545b87b996b'
            )
            or (
                market_structure = 'neg_risk'
                and exchange_address
                <> '0xe2222d279d744050d28e00520010520000310f59'
            )
            or (
                stage = 'group_stage'
                and (
                    market_structure <> 'neg_risk'
                    or exchange_address
                    <> '0xe2222d279d744050d28e00520010520000310f59'
                )
            )
            or (
                stage <> 'group_stage'
                and (
                    market_structure <> 'standard'
                    or exchange_address
                    <> '0xe111180000d2663c0091e4f400237545b87b996b'
                )
            )
        ) as invalid_market_id_rows,
        count(*) filter (
            where
            coalesce(openfootball_revision, '')
            <> 'bd46a148289f9930da66c140d4d7d2325e95d387'
            or coalesce(openfootball_path, '') not in (
                '2026--usa/cup.txt',
                '2026--usa/cup_finals.txt'
            )
            or nullif(openfootball_source_lines, '') is null
            or not regexp_full_match(
                coalesce(openfootball_line_hash, ''), '[0-9a-f]{64}'
            )
            or not regexp_full_match(
                coalesce(condition_init_tx_hash, ''), '0x[0-9a-f]{64}'
            )
            or coalesce(condition_init_log_index, -1) < 0
            or not regexp_full_match(
                coalesce(question_init_tx_hash, ''), '0x[0-9a-f]{64}'
            )
            or coalesce(question_init_log_index, -1) < 0
            or not regexp_full_match(
                coalesce(ancillary_data_sha256, ''), '[0-9a-f]{64}'
            )
            or coalesce(token_verification_block_number, -1) < 0
            or not regexp_full_match(
                coalesce(token_verification_block_hash, ''),
                '0x[0-9a-f]{64}'
            )
        ) as invalid_evidence_rows,
        count(*) filter (
            where
            lower(concat_ws(
                ' ', yes_represents, no_represents, openfootball_path
            )) similar to '%(gamma|clob|polymarket\.com|event_slug|market_slug)%'
        ) as prohibited_source_rows
    from seed
),

token_summary as (
    select
        count(*) as seed_token_rows,
        count(distinct token_id) as seed_tokens
    from seed_tokens
),

match_shape_summary as (
    select
        count(*) filter (
            where
            stage_count <> 1
            or home_team_count <> 1
            or away_team_count <> 1
            or kickoff_count <> 1
            or window_start_count <> 1
            or window_end_count <> 1
            or (
                stage = 'group_stage'
                and (
                    fifa_match_id not between 1 and 72
                    or group_count <> 1
                    or proposition_count <> 3
                    or proposition_type_count <> 3
                    or valid_group_proposition_count <> 3
                )
            )
            or (
                stage <> 'group_stage'
                and (
                    group_count <> 0
                    or (fifa_match_id between 73 and 88 and stage <> 'round_of_32')
                    or (fifa_match_id between 89 and 96 and stage <> 'round_of_16')
                    or (fifa_match_id between 97 and 100 and stage <> 'quarterfinal')
                    or (fifa_match_id between 101 and 102 and stage <> 'semifinal')
                    or (fifa_match_id = 103 and stage <> 'third_place')
                    or (fifa_match_id = 104 and stage <> 'final')
                    or proposition_count <> 1
                    or proposition_type_count <> 1
                    or valid_advance_proposition_count
                    + valid_third_place_proposition_count
                    + valid_final_proposition_count <> 1
                )
            )
        ) as invalid_match_shapes
    from match_shape
),

published_scans as (
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
    from published_scans
    order by published_at desc nulls last, finished_at desc nulls last, scan_id desc
    limit 1
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
),

current_fills as (
    select fills.*
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_fills') }} as fills
    inner join latest_published_scan as scan on fills.scan_id = scan.scan_id
),

priced_fills as (
    select
        fills.*,
        try_cast(fills.source_maker_amount as hugeint)
            as source_maker_amount_int,
        try_cast(fills.source_taker_amount as hugeint)
            as source_taker_amount_int,
        cast(fills.share_volume * 1000000 as hugeint)
            as normalized_share_amount_int,
        cast(fills.gross_collateral_volume * 1000000 as hugeint)
            as normalized_collateral_amount_int,
        {{ polygon_settlement_ratio_half_even(
            'fills.gross_collateral_volume',
            'fills.share_volume'
        ) }} as expected_price
    from current_fills as fills
),

foreign_scan_fills as (
    select count(*) as foreign_scan_fill_rows
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_fills') }} as fills
    left join latest_published_scan as scan on fills.scan_id = scan.scan_id
    where scan.scan_id is null
),

duplicate_fills as (
    select count(*) as duplicate_fill_grains
    from (
        select
            chain_id,
            exchange_address,
            transaction_hash,
            passive_log_index,
            normalized_leg_ordinal
        from current_fills
        group by all
        having count(*) > 1
    ) as duplicates
),

normalization_pair_grains as (
    select
        scan_id,
        chain_id,
        exchange_address,
        transaction_hash,
        passive_log_index,
        count(*) as leg_count,
        count(distinct normalized_leg_ordinal) as ordinal_count,
        count(*) filter (
            where normalized_leg_ordinal = 0 and not is_derived
        ) as base_leg_count,
        count(*) filter (
            where normalized_leg_ordinal = 1 and is_derived
        ) as derived_leg_count,
        count(distinct token_id) as token_count,
        count(distinct outcome_side) as outcome_count,
        count(*) filter (
            where not is_derived and token_id = source_token_id
        ) as source_leg_count,
        count(*) filter (
            where is_derived and token_id <> source_token_id
        ) as derived_counterpart_count,
        min(share_volume) as minimum_share_volume,
        max(share_volume) as maximum_share_volume,
        sum(gross_collateral_volume) as total_collateral_volume,
        count(distinct row(
            chunk_from_block,
            chunk_to_block,
            block_number,
            block_hash,
            block_timestamp,
            transaction_index,
            active_log_index,
            matched_log_index
        )) as locator_variant_count,
        count(distinct row(
            proposition_id,
            condition_id,
            order_side,
            source_token_id,
            source_maker_amount,
            source_taker_amount,
            normalization_kind,
            segment_sha256,
            decoder_version,
            ingested_at
        )) as segment_variant_count,
        count(*) filter (
            where
            (normalization_kind = 'mint' and order_side <> 'BUY')
            or (normalization_kind = 'merge' and order_side <> 'SELL')
        ) as kind_side_mismatch_count
    from current_fills
    where normalization_kind in ('mint', 'merge')
    group by all
),

normalization_pair_summary as (
    select
        count(*) filter (
            where
            leg_count <> 2
            or ordinal_count <> 2
            or base_leg_count <> 1
            or derived_leg_count <> 1
            or token_count <> 2
            or outcome_count <> 2
            or source_leg_count <> 1
            or derived_counterpart_count <> 1
            or minimum_share_volume <> maximum_share_volume
            or total_collateral_volume <> maximum_share_volume
            or locator_variant_count <> 1
            or segment_variant_count <> 1
            or kind_side_mismatch_count > 0
        ) as invalid_normalization_pair_grains
    from normalization_pair_grains
),

fill_validation as (
    select
        count(*) as raw_fill_rows,
        count(*) filter (
            where
            universe.proposition_id is null
            or coalesce(fills.chain_id, -1) <> 137
            or fills.condition_id is null
            or fills.condition_id <> universe.condition_id
            or fills.exchange_address is null
            or fills.exchange_address <> universe.exchange_address
            or fills.token_id is null
            or coalesce(fills.outcome_side, '') not in ('yes', 'no')
            or (
                fills.token_id = universe.yes_token_id
                and fills.outcome_side <> 'yes'
            )
            or (
                fills.token_id = universe.no_token_id
                and fills.outcome_side <> 'no'
            )
            or fills.token_id not in (
                universe.yes_token_id, universe.no_token_id
            )
            or coalesce(fills.order_side, '') not in ('BUY', 'SELL')
        ) as invalid_fill_mapping_rows,
        count(*) filter (
            where
            fills.block_timestamp is null
            or fills.block_timestamp < universe.analysis_window_start_at_utc
            or fills.block_timestamp >= universe.analysis_window_end_at_utc
            or fills.price is null
            or fills.price not between 0 and 1
            or coalesce(fills.share_volume, 0) <= 0
            or coalesce(fills.gross_collateral_volume, 0) <= 0
            or fills.share_volume
            > cast('340282366920938.463374' as decimal(38, 6))
            or fills.gross_collateral_volume
            > cast('340282366920938.463374' as decimal(38, 6))
            or fills.gross_collateral_volume > fills.share_volume
            or fills.expected_price is null
            or fills.price <> fills.expected_price
            or fills.is_derived is null
            or coalesce(fills.block_number, -1) < 0
            or coalesce(fills.transaction_index, -1) < 0
            or coalesce(fills.passive_log_index, -1) < 0
            or fills.active_log_index <= fills.passive_log_index
            or fills.matched_log_index <= fills.active_log_index
            or coalesce(fills.active_log_index, -1) < 0
            or coalesce(fills.matched_log_index, -1) < 0
            or coalesce(fills.normalized_leg_ordinal, -1) not between 0 and 1
            or coalesce(fills.normalization_kind, '') not in (
                'complementary', 'mint', 'merge'
            )
            or (fills.is_derived and fills.normalization_kind = 'complementary')
            or (
                fills.normalization_kind = 'complementary'
                and (
                    fills.is_derived
                    or fills.normalized_leg_ordinal <> 0
                    or fills.source_token_id <> fills.token_id
                )
            )
            or (
                not fills.is_derived
                and (
                    fills.normalized_leg_ordinal <> 0
                    or fills.source_token_id <> fills.token_id
                    or fills.source_maker_amount_int is null
                    or fills.source_taker_amount_int is null
                    or (
                        fills.order_side = 'BUY'
                        and (
                            fills.source_taker_amount_int
                            <> fills.normalized_share_amount_int
                            or fills.source_maker_amount_int
                            <> fills.normalized_collateral_amount_int
                        )
                    )
                    or (
                        fills.order_side = 'SELL'
                        and (
                            fills.source_maker_amount_int
                            <> fills.normalized_share_amount_int
                            or fills.source_taker_amount_int
                            <> fills.normalized_collateral_amount_int
                        )
                    )
                )
            )
            or not regexp_full_match(
                coalesce(fills.source_token_id, ''),
                '[1-9][0-9]{0,77}'
            )
            or not regexp_full_match(
                coalesce(fills.source_maker_amount, ''),
                '[1-9][0-9]{0,77}'
            )
            or not regexp_full_match(
                coalesce(fills.source_taker_amount, ''),
                '[1-9][0-9]{0,77}'
            )
            or not regexp_full_match(
                coalesce(fills.block_hash, ''), '0x[0-9a-f]{64}'
            )
            or not regexp_full_match(
                coalesce(fills.transaction_hash, ''), '0x[0-9a-f]{64}'
            )
            or not regexp_full_match(
                coalesce(fills.segment_sha256, ''), '[0-9a-f]{64}'
            )
            or coalesce(fills.decoder_version, '')
            <> 'polygon-v2-settlement-v4'
        ) as invalid_fill_value_rows,
        count(*) filter (where chunks.scan_id is null) as unmatched_fill_chunks
    from priced_fills as fills
    left join seed as universe on fills.proposition_id = universe.proposition_id
    left join
        {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_chunks') }}
            as chunks
        on
            fills.scan_id = chunks.scan_id
            and fills.exchange_address = chunks.exchange_address
            and fills.chunk_from_block = chunks.from_block
            and fills.chunk_to_block = chunks.to_block
            and chunks.status = 'success'
),

candidate_by_proposition as (
    select
        proposition_id,
        max(case when stage = 'group_stage' then 150 else 210 end)
            as expected_rows,
        count(*) as actual_rows,
        count(distinct elapsed_window_minute) as distinct_minutes,
        min(elapsed_window_minute) as first_minute,
        max(elapsed_window_minute) as last_minute,
        count(*) filter (
            where
            settlement_minute_utc
            <> analysis_window_start_at_utc
            + elapsed_window_minute * interval '1 minute'
            or settlement_minute_utc >= analysis_window_end_at_utc
        ) as invalid_axis_rows
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
    group by proposition_id
),

candidate_distinct_grains as (
    select distinct
        proposition_id,
        settlement_minute_epoch
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
),

candidate_distinct_grain_summary as (
    select count(*) as distinct_minute_grains
    from candidate_distinct_grains
),

candidate_summary as (
    select
        count(*) as actual_minute_rows,
        sum(yes_normalized_fill_count + no_normalized_fill_count)
            as candidate_fill_count,
        count(*) filter (
            where
            yes_observed is null
            or no_observed is null
            or yes_derived_fill_count > yes_normalized_fill_count
            or no_derived_fill_count > no_normalized_fill_count
            or (
                yes_observed
                and (
                    yes_normalized_fill_count <= 0
                    or yes_share_volume <= 0
                    or yes_gross_collateral_volume <= 0
                    or yes_open_price is null
                    or yes_high_price is null
                    or yes_low_price is null
                    or yes_close_price is null
                    or yes_vwap is null
                    or yes_first_settlement_at_utc is null
                    or yes_last_settlement_at_utc is null
                    or yes_first_settlement_at_utc
                    > yes_last_settlement_at_utc
                )
            )
            or (
                no_observed
                and (
                    no_normalized_fill_count <= 0
                    or no_share_volume <= 0
                    or no_gross_collateral_volume <= 0
                    or no_open_price is null
                    or no_high_price is null
                    or no_low_price is null
                    or no_close_price is null
                    or no_vwap is null
                    or no_first_settlement_at_utc is null
                    or no_last_settlement_at_utc is null
                    or no_first_settlement_at_utc
                    > no_last_settlement_at_utc
                )
            )
            or (
                not yes_observed
                and (
                    yes_normalized_fill_count <> 0
                    or yes_derived_fill_count <> 0
                    or yes_share_volume <> 0
                    or yes_gross_collateral_volume <> 0
                    or yes_open_price is not null
                    or yes_high_price is not null
                    or yes_low_price is not null
                    or yes_close_price is not null
                    or yes_vwap is not null
                    or yes_first_settlement_at_utc is not null
                    or yes_last_settlement_at_utc is not null
                )
            )
            or (
                not no_observed
                and (
                    no_normalized_fill_count <> 0
                    or no_derived_fill_count <> 0
                    or no_share_volume <> 0
                    or no_gross_collateral_volume <> 0
                    or no_open_price is not null
                    or no_high_price is not null
                    or no_low_price is not null
                    or no_close_price is not null
                    or no_vwap is not null
                    or no_first_settlement_at_utc is not null
                    or no_last_settlement_at_utc is not null
                )
            )
            or minute_complete is null
            or minute_complete <> (yes_observed and no_observed)
            or coalesce(minute_status, '') <> case
                when yes_observed and no_observed then 'both_observed'
                when yes_observed then 'yes_only'
                when no_observed then 'no_only'
                else 'no_fills'
            end
        ) as invalid_candidate_state_rows
    from {{ ref('int_polymarket_wc2026_polygon_settlement_minute_odds_candidate') }}
),

axis_summary as (
    select
        count(*) filter (
            where
            actual_rows <> expected_rows
            or distinct_minutes <> expected_rows
            or first_minute <> 0
            or last_minute <> expected_rows - 1
            or invalid_axis_rows > 0
        ) as invalid_proposition_axes
    from candidate_by_proposition
),

issue_summary as (
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
        token_summary.*,
        match_shape_summary.*,
        scan_summary.*,
        chunk_summary.*,
        foreign_scan_fills.*,
        duplicate_fills.*,
        normalization_pair_summary.*,
        fill_validation.*,
        candidate_summary.*,
        candidate_distinct_grain_summary.*,
        axis_summary.*,
        issue_summary.*
    from seed_summary
    cross join token_summary
    cross join match_shape_summary
    cross join scan_summary
    cross join chunk_summary
    cross join foreign_scan_fills
    cross join duplicate_fills
    cross join normalization_pair_summary
    cross join fill_validation
    cross join candidate_summary
    cross join candidate_distinct_grain_summary
    cross join axis_summary
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
