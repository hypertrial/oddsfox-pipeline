{{ config(materialized='table', tags=['pmxt_order_book']) }}

with snapshots as (
    select *
    from {{ ref('stg_polymarket_wc2026_match_order_book_snapshots') }}
),

fixture as (
    select *
    from {{ ref('stg_openfootball_wc2026_knockout_fixtures') }}
),

expanded as (
    select
        snapshots.*,
        fixture.stage_key as fixture_stage,
        fixture.home_team as fixture_home_team,
        fixture.away_team as fixture_away_team,
        'bid' as book_side,
        cast(book_level.key as integer) as source_level_ordinal,
        json_extract_string(book_level.value, '$.price') as price_raw,
        json_extract_string(book_level.value, '$.size') as size_raw,
        try_cast(json_extract_string(book_level.value, '$.price') as decimal(38, 18)) as price,
        try_cast(json_extract_string(book_level.value, '$.size') as decimal(38, 18)) as size,
        try_cast(json_extract_string(book_level.value, '$.order_count') as bigint) as order_count,
        json_type(book_level.value, '$.order_count') as order_count_type
    from snapshots
    left join fixture
        on snapshots.fifa_match_id = fixture.fifa_match_id
    cross join
        lateral json_each(
            case when snapshots.bids_json_valid then snapshots.bids_json else '[]' end
        ) as book_level

    union all

    select
        snapshots.*,
        fixture.stage_key as fixture_stage,
        fixture.home_team as fixture_home_team,
        fixture.away_team as fixture_away_team,
        'ask' as book_side,
        cast(book_level.key as integer) as source_level_ordinal,
        json_extract_string(book_level.value, '$.price') as price_raw,
        json_extract_string(book_level.value, '$.size') as size_raw,
        try_cast(json_extract_string(book_level.value, '$.price') as decimal(38, 18)) as price,
        try_cast(json_extract_string(book_level.value, '$.size') as decimal(38, 18)) as size,
        try_cast(json_extract_string(book_level.value, '$.order_count') as bigint) as order_count,
        json_type(book_level.value, '$.order_count') as order_count_type
    from snapshots
    left join fixture
        on snapshots.fifa_match_id = fixture.fifa_match_id
    cross join lateral json_each(
        case when snapshots.asks_json_valid then snapshots.asks_json else '[]' end
    ) as book_level
),

ranked as (
    select
        *,
        row_number() over (
            partition by
                scan_id,
                clob_token_id,
                snapshot_timestamp_ms,
                snapshot_sha256,
                book_side
            order by
                case when book_side = 'bid' then price end desc nulls last,
                case when book_side = 'ask' then price end asc nulls last,
                source_level_ordinal
        ) as level_rank,
        max(price) filter (where book_side = 'bid') over (
            partition by
                scan_id, clob_token_id, snapshot_timestamp_ms, snapshot_sha256
        ) as best_bid_price,
        min(price) filter (where book_side = 'ask') over (
            partition by
                scan_id, clob_token_id, snapshot_timestamp_ms, snapshot_sha256
        ) as best_ask_price
    from expanded
)

select
    scan_id,
    manifest_sha256,
    fifa_match_id,
    stage,
    home_team,
    away_team,
    event_id,
    event_slug,
    market_id,
    market_slug,
    market_type,
    condition_id,
    outcome_label,
    landscape_role,
    clob_token_id,
    window_start_ms,
    window_end_ms,
    snapshot_timestamp_ms,
    snapshot_at_utc,
    snapshot_sha256,
    provider_sequence,
    book_side,
    level_rank,
    price_raw,
    size_raw,
    price,
    size,
    order_count,
    order_count_type,
    cast(
        cast(price as decimal(20, 18))
        * cast(size as decimal(18, 6))
        as decimal(38, 18)
    ) as level_notional,
    best_bid_price,
    best_ask_price,
    last_trade_price,
    is_neg_risk,
    source_endpoint,
    ingested_at,
    fixture_stage,
    fixture_home_team,
    fixture_away_team,
    bids_json_valid,
    asks_json_valid,
    sum(size) over (
        partition by
            scan_id,
            clob_token_id,
            snapshot_timestamp_ms,
            snapshot_sha256,
            book_side
        order by level_rank
        rows between unbounded preceding and current row
    ) as cumulative_size,
    sum(cast(
        cast(price as decimal(20, 18))
        * cast(size as decimal(18, 6))
        as decimal(38, 18)
    )) over (
        partition by
            scan_id,
            clob_token_id,
            snapshot_timestamp_ms,
            snapshot_sha256,
            book_side
        order by level_rank
        rows between unbounded preceding and current row
    ) as cumulative_notional,
    best_ask_price - best_bid_price as spread,
    (best_ask_price + best_bid_price) / 2 as midpoint
from ranked
