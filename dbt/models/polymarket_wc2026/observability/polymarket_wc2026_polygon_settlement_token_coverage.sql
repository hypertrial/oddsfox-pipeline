{{ config(materialized='table', tags=['polygon_settlement']) }}

with mapped_tokens as (
    select
        proposition_id,
        fifa_match_id,
        'yes' as token_side,
        yes_token_id as token_id,
        window_minutes as expected_minute_buckets
    from {{ ref('int_polymarket_wc2026_polygon_settlement_market_universe') }}

    union all

    select
        proposition_id,
        fifa_match_id,
        'no' as token_side,
        no_token_id as token_id,
        window_minutes as expected_minute_buckets
    from {{ ref('int_polymarket_wc2026_polygon_settlement_market_universe') }}
),

observed as (
    select  -- noqa: ST06
        proposition_id,
        token_id,
        count(*) as observed_minute_buckets,
        sum(normalized_fill_count) as normalized_fill_count,
        sum(derived_fill_count) as derived_fill_count,
        cast(sum(share_volume) as decimal(38, 6)) as share_volume,
        cast(sum(gross_collateral_volume) as decimal(38, 6))
            as gross_collateral_volume,
        min(first_settlement_at_utc) as first_settlement_at_utc,
        max(last_settlement_at_utc) as last_settlement_at_utc
    from {{ ref('int_polymarket_wc2026_polygon_settlement_token_minute_odds') }}
    group by proposition_id, token_id
)

select  -- noqa: ST06
    tokens.proposition_id,
    tokens.fifa_match_id,
    tokens.token_side,
    tokens.token_id,
    tokens.expected_minute_buckets,
    coalesce(observed.observed_minute_buckets, 0) as observed_minute_buckets,
    coalesce(observed.normalized_fill_count, 0) as normalized_fill_count,
    coalesce(observed.derived_fill_count, 0) as derived_fill_count,
    coalesce(observed.share_volume, cast(0 as decimal(38, 6)))
        as share_volume,
    coalesce(observed.gross_collateral_volume, cast(0 as decimal(38, 6)))
        as gross_collateral_volume,
    observed.first_settlement_at_utc,
    observed.last_settlement_at_utc,
    coalesce(
        cast(observed.observed_minute_buckets as double)
        / nullif(tokens.expected_minute_buckets, 0),
        0
    ) as minute_coverage_ratio,
    observed.normalized_fill_count is not null as has_any_fill
from mapped_tokens as tokens
left join observed
    on
        tokens.proposition_id = observed.proposition_id
        and tokens.token_id = observed.token_id
