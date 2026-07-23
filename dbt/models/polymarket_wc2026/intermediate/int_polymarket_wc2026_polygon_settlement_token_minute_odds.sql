{{ config(materialized='table', tags=['polygon_settlement']) }}

with latest_published_scan as (
    select scan_id
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_scan_runs') }}
    where status = 'published' and raw_published
    order by published_at desc nulls last, finished_at desc nulls last, scan_id desc
    limit 1
),

selected_tokens as (
    select
        proposition_id,
        condition_id,
        'yes' as outcome_side,
        yes_token_id as token_id,
        analysis_window_start_at_utc,
        analysis_window_end_at_utc
    from {{ ref('int_polymarket_wc2026_polygon_settlement_market_universe') }}

    union all

    select
        proposition_id,
        condition_id,
        'no' as outcome_side,
        no_token_id as token_id,
        analysis_window_start_at_utc,
        analysis_window_end_at_utc
    from {{ ref('int_polymarket_wc2026_polygon_settlement_market_universe') }}
),

in_window as (
    select  -- noqa: ST06
        fills.proposition_id,
        fills.token_id,
        tokens.outcome_side,
        date_trunc('minute', fills.block_timestamp) as settlement_minute_utc,
        fills.block_timestamp as settlement_at_utc,
        fills.block_number,
        fills.transaction_index,
        fills.passive_log_index,
        fills.normalized_leg_ordinal,
        fills.price,
        fills.share_volume,
        fills.gross_collateral_volume,
        fills.is_derived
    from {{ ref('stg_polymarket_wc2026_polygon_settlement_fills') }} as fills
    inner join latest_published_scan as scan
        on fills.scan_id = scan.scan_id
    inner join selected_tokens as tokens
        on
            fills.proposition_id = tokens.proposition_id
            and fills.condition_id = tokens.condition_id
            and fills.token_id = tokens.token_id
            and fills.outcome_side = tokens.outcome_side
    where
        fills.block_timestamp >= tokens.analysis_window_start_at_utc
        and fills.block_timestamp < tokens.analysis_window_end_at_utc
),

ranked as (
    select
        *,
        row_number() over (
            partition by token_id, settlement_minute_utc
            order by
                block_number,
                transaction_index,
                passive_log_index,
                normalized_leg_ordinal
        ) as open_rank,
        row_number() over (
            partition by token_id, settlement_minute_utc
            order by
                block_number desc,
                transaction_index desc,
                passive_log_index desc,
                normalized_leg_ordinal desc
        ) as close_rank
    from in_window
),

aggregated as (
    select  -- noqa: ST06
        proposition_id,
        token_id,
        outcome_side,
        settlement_minute_utc,
        cast(epoch(settlement_minute_utc) as bigint) as settlement_minute_epoch,
        max(case when open_rank = 1 then price end) as open_price,
        max(price) as high_price,
        min(price) as low_price,
        max(case when close_rank = 1 then price end) as close_price,
        count(*) as normalized_fill_count,
        count(*) filter (where is_derived) as derived_fill_count,
        cast(sum(share_volume) as decimal(38, 6)) as share_volume,
        cast(sum(gross_collateral_volume) as decimal(38, 6))
            as gross_collateral_volume,
        min(settlement_at_utc) as first_settlement_at_utc,
        max(settlement_at_utc) as last_settlement_at_utc
    from ranked
    group by proposition_id, token_id, outcome_side, settlement_minute_utc
)

select  -- noqa: ST06
    proposition_id,
    token_id,
    outcome_side,
    settlement_minute_utc,
    settlement_minute_epoch,
    open_price,
    high_price,
    low_price,
    close_price,
    {{ polygon_settlement_ratio_half_even(
        'gross_collateral_volume',
        'share_volume'
    ) }} as vwap,
    normalized_fill_count,
    derived_fill_count,
    share_volume,
    gross_collateral_volume,
    first_settlement_at_utc,
    last_settlement_at_utc
from aggregated
