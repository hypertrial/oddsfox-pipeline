{{ config(materialized='table', tags=['polygon_settlement']) }}

with spine as (
    select
        universe.*,
        minute_spine.elapsed_window_minute,
        universe.analysis_window_start_at_utc
        + minute_spine.elapsed_window_minute * interval '1 minute'
            as settlement_minute_utc
    from
        {{ ref('int_polymarket_wc2026_polygon_settlement_market_universe') }}
            as universe
    -- costguard: allow cross-join, exactly 150 or 210 bounded rows per proposition.
    cross join range(0, universe.window_minutes)
        as minute_spine (elapsed_window_minute)
),

joined as (
    select  -- noqa: ST06
        spine.proposition_id,
        spine.fifa_match_id,
        spine.stage,
        spine.group_name,
        spine.home_team,
        spine.away_team,
        spine.proposition_type,
        spine.yes_represents,
        spine.no_represents,
        spine.scheduled_kickoff_at_utc,
        spine.analysis_window_start_at_utc,
        spine.analysis_window_end_at_utc,
        spine.settlement_minute_utc,
        cast(epoch(spine.settlement_minute_utc) as bigint)
            as settlement_minute_epoch,
        spine.elapsed_window_minute,
        spine.condition_id,
        spine.yes_token_id,
        spine.no_token_id,
        spine.market_structure,
        spine.exchange_address,
        spine.manifest_sha256,
        spine.manifest_version,
        yes_odds.open_price as yes_open_price,
        yes_odds.high_price as yes_high_price,
        yes_odds.low_price as yes_low_price,
        yes_odds.close_price as yes_close_price,
        yes_odds.vwap as yes_vwap,
        coalesce(yes_odds.normalized_fill_count, 0) as yes_normalized_fill_count,
        coalesce(yes_odds.derived_fill_count, 0) as yes_derived_fill_count,
        coalesce(
            yes_odds.share_volume, cast(0 as decimal(38, 6))
        ) as yes_share_volume,
        coalesce(
            yes_odds.gross_collateral_volume, cast(0 as decimal(38, 6))
        ) as yes_gross_collateral_volume,
        yes_odds.first_settlement_at_utc as yes_first_settlement_at_utc,
        yes_odds.last_settlement_at_utc as yes_last_settlement_at_utc,
        yes_odds.token_id is not null as yes_observed,
        no_odds.open_price as no_open_price,
        no_odds.high_price as no_high_price,
        no_odds.low_price as no_low_price,
        no_odds.close_price as no_close_price,
        no_odds.vwap as no_vwap,
        coalesce(no_odds.normalized_fill_count, 0) as no_normalized_fill_count,
        coalesce(no_odds.derived_fill_count, 0) as no_derived_fill_count,
        coalesce(
            no_odds.share_volume, cast(0 as decimal(38, 6))
        ) as no_share_volume,
        coalesce(
            no_odds.gross_collateral_volume, cast(0 as decimal(38, 6))
        ) as no_gross_collateral_volume,
        no_odds.first_settlement_at_utc as no_first_settlement_at_utc,
        no_odds.last_settlement_at_utc as no_last_settlement_at_utc,
        no_odds.token_id is not null as no_observed
    from spine
    left join
        {{ ref('int_polymarket_wc2026_polygon_settlement_token_minute_odds') }}
            as yes_odds
        on
            spine.proposition_id = yes_odds.proposition_id
            and spine.yes_token_id = yes_odds.token_id
            and spine.settlement_minute_utc = yes_odds.settlement_minute_utc
    left join
        {{ ref('int_polymarket_wc2026_polygon_settlement_token_minute_odds') }}
            as no_odds
        on
            spine.proposition_id = no_odds.proposition_id
            and spine.no_token_id = no_odds.token_id
            and spine.settlement_minute_utc = no_odds.settlement_minute_utc
)

select
    *,
    yes_observed and no_observed as minute_complete,
    case
        when yes_observed and no_observed then 'both_observed'
        when yes_observed then 'yes_only'
        when no_observed then 'no_only'
        else 'no_fills'
    end as minute_status
from joined
