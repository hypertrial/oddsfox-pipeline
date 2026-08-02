{{ config(materialized='table', tags=['polygon_settlement']) }}

with latest_published_scan as (
    select *
    from {{ ref('int_polymarket_wc2026_polygon_settlement_latest_published_scan') }}
),

seed as (
    select *
    from {{ ref('int_polymarket_wc2026_polygon_settlement_market_universe') }}
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
)

select
    foreign_scan_fills.*,
    duplicate_fills.*,
    normalization_pair_summary.*,
    fill_validation.*
from foreign_scan_fills
cross join duplicate_fills
cross join normalization_pair_summary
cross join fill_validation
