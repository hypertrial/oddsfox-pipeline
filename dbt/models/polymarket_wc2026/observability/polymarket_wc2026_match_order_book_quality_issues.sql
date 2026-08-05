{{ config(materialized='table', tags=['pmxt_order_book']) }}
-- noqa: disable=AL03

with snapshots as (
    select *
    from {{ ref('stg_polymarket_wc2026_match_order_book_snapshots') }}
),

levels as (
    select *
    from {{ ref('int_polymarket_wc2026_match_order_book_levels') }}
),

inventory as (
    select
        count(*) as snapshot_count,
        count(distinct scan_id) as scan_count,
        count(distinct manifest_sha256) as manifest_count,
        count(distinct fifa_match_id) as match_count,
        min(fifa_match_id) as fifa_match_id,
        count(distinct market_id) as market_count,
        count(distinct clob_token_id) as token_count,
        sum(
            case
                when bids_json_valid then cast(json_array_length(bids_json) as bigint)
                else 0
            end
            + case
                when asks_json_valid then cast(json_array_length(asks_json) as bigint)
                else 0
            end
        ) as expected_level_count,
        count(*) filter (where not bids_json_valid or not asks_json_valid) as invalid_json_count,
        count(*) filter (
            where
            fifa_match_id not between 1 and 104
            or landscape_role not in (
                'home', 'away', 'home_win', 'draw', 'away_win'
            )
            or condition_id is null
            or home_team is null
            or away_team is null
        ) as identity_issue_count,
        count(*) filter (
            where
            snapshot_timestamp_ms < window_start_ms
            or snapshot_timestamp_ms > window_end_ms
        ) as timestamp_issue_count,
        count(*) filter (
            where
            last_trade_price_raw is not null
            and (
                not regexp_full_match(
                    last_trade_price_raw,
                    '(0|[1-9][0-9]{0,19})([.][0-9]{1,18})?'
                )
                or
                last_trade_price is null
                or last_trade_price < 0
                or last_trade_price > 1
            )
        ) as last_trade_price_issue_count,
        count(*) filter (
            where
            bids_json_valid
            and asks_json_valid
            and json_array_length(bids_json) = 0
            and json_array_length(asks_json) = 0
        ) as empty_book_count
    from snapshots
),

token_identity as (
    select count(*) as issue_count
    from (
        select clob_token_id
        from snapshots
        group by clob_token_id
        having
            count(distinct outcome_label) != 1
            or count(distinct landscape_role) != 1
    ) as inconsistent_tokens
),

scan_contract as (
    select count(*) as issue_count
    from (
        select distinct
            scan_id,
            manifest_sha256
        from snapshots
    ) as snapshots
    inner join {{ source('polymarket_wc2026_ops', 'match_order_book_scan_runs') }} as runs
        on snapshots.scan_id = runs.scan_id
    cross join inventory
    where
        runs.status != 'published'
        or not runs.raw_published
        or runs.aggregate_sha256 is null
        or runs.snapshot_count != inventory.snapshot_count
        or runs.token_count != inventory.token_count
        or runs.target_count != inventory.market_count
        or runs.manifest_sha256 != snapshots.manifest_sha256
),

advancement_fixtures as (
    select *
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }}
    where fifa_match_id between 73 and 104
),

fixture_contract as (
    select count(*) as issue_count
    from snapshots
    left join advancement_fixtures as fixture
        on snapshots.fifa_match_id = fixture.fifa_match_id
    where
        snapshots.fifa_match_id >= 73
        and (
            fixture.fifa_match_id is null
            or fixture.stage_key != snapshots.stage
            or fixture.home_team != snapshots.home_team
            or fixture.away_team != snapshots.away_team
        )
),

duplicate_snapshots as (
    select count(*) as issue_count
    from (
        select
            scan_id,
            clob_token_id,
            snapshot_timestamp_ms,
            snapshot_sha256
        from snapshots
        group by all
        having count(*) > 1
    ) as duplicate_snapshot_groups
),

duplicate_level_grain as (
    select count(*) as issue_count
    from (
        select
            fifa_match_id,
            market_id,
            clob_token_id,
            snapshot_timestamp_ms,
            snapshot_sha256,
            book_side,
            level_rank
        from levels
        group by all
        having count(*) > 1
    ) as duplicate_level_groups
),

duplicate_side_prices as (
    select count(*) as issue_count
    from (
        select
            clob_token_id,
            snapshot_timestamp_ms,
            snapshot_sha256,
            book_side,
            price
        from levels
        group by all
        having count(*) > 1
    ) as duplicate_side_price_groups
),

rank_issues as (
    select count(*) as issue_count
    from (
        select
            clob_token_id,
            snapshot_timestamp_ms,
            snapshot_sha256,
            book_side
        from levels
        group by all
        having min(level_rank) != 1 or max(level_rank) != count(*)
    ) as invalid_rank_groups
),

recomputed_depth as (
    select
        *,
        sum(size) over (
            partition by
                scan_id,
                clob_token_id,
                snapshot_timestamp_ms,
                snapshot_sha256,
                book_side
            order by level_rank
            rows between unbounded preceding and current row
        ) as expected_cumulative_size,
        sum(level_notional) over (
            partition by
                scan_id,
                clob_token_id,
                snapshot_timestamp_ms,
                snapshot_sha256,
                book_side
            order by level_rank
            rows between unbounded preceding and current row
        ) as expected_cumulative_notional
    from levels
),

level_issues as (
    select
        count(*) as level_count,
        count(*) filter (
            where
            price is null or price < 0 or price > 1
            or size is null or size <= 0
            or not coalesce(
                regexp_full_match(
                    price_raw,
                    '(0|[1-9][0-9]{0,19})([.][0-9]{1,18})?'
                ),
                false
            )
            or not coalesce(
                regexp_full_match(
                    size_raw,
                    '(0|[1-9][0-9]{0,19})([.][0-9]{1,18})?'
                ),
                false
            )
            or order_count < 0
            or coalesce(order_count_type not in ('NULL', 'UBIGINT'), true)
        ) as numeric_issue_count,
        count(*) filter (
            where
            cumulative_size != expected_cumulative_size
            or cumulative_notional != expected_cumulative_notional
        ) as cumulative_issue_count
    from recomputed_depth
),

crossed_books as (
    select count(*) as issue_count
    from (
        select distinct
            scan_id,
            clob_token_id,
            snapshot_timestamp_ms,
            snapshot_sha256
        from levels
        where
            best_bid_price is not null
            and best_ask_price is not null
            and best_bid_price > best_ask_price
    ) as crossed_snapshot_books
),

window_issues as (
    select count(*) as issue_count
    from {{ source('polymarket_wc2026_ops', 'match_order_book_scan_windows') }} as windows
    where
        windows.scan_id = (select min(snapshots.scan_id) from snapshots)
        and windows.status in ('pending', 'failed')
),

snapshot_gaps as (
    select count(*) as issue_count
    from (
        select
            snapshot_timestamp_ms
            - lag(snapshot_timestamp_ms) over (
                partition by clob_token_id
                order by snapshot_timestamp_ms, snapshot_sha256
            ) as gap_ms
        from snapshots
    ) as token_snapshot_gaps
    where gap_ms > 6 * 60 * 60 * 1000
),

issues as (
    select
        'inventory' as issue_key,
        'error' as severity,
        1 as affected_rows,
        'expected FIFA match 95 with 1 advance market and 2 tokens' as details
    from inventory
    where
        snapshot_count = 0
        or scan_count != 1
        or manifest_count != 1
        or match_count != 1
        or fifa_match_id != 95
        or market_count != 1
        or token_count != 2

    union all

    select
        'level_inventory' as issue_key,
        'error' as severity,
        abs(inventory.expected_level_count - level_issues.level_count) as affected_rows,
        'raw bid/ask array length does not equal exploded level inventory' as details
    from inventory
    cross join level_issues
    where inventory.expected_level_count != level_issues.level_count

    union all

    select
        'target_identity',
        'error',
        identity_issue_count,
        'fixed target identity mismatch'
    from inventory
    where identity_issue_count > 0

    union all

    select
        'token_identity',
        'error',
        issue_count,
        'outcome/token mapping mismatch'
    from token_identity
    where issue_count > 0

    union all

    select
        'scan_contract',
        'error',
        issue_count,
        'published scan inventory or manifest mismatch'
    from scan_contract
    where issue_count > 0

    union all

    select
        'fixture_identity',
        'error',
        issue_count,
        'OpenFootball fixture mismatch'
    from fixture_contract
    where issue_count > 0

    union all

    select
        'timestamp_bounds',
        'error',
        timestamp_issue_count,
        'snapshot outside approved history window'
    from inventory
    where timestamp_issue_count > 0

    union all

    select
        'invalid_json',
        'error',
        invalid_json_count,
        'invalid canonical side JSON'
    from inventory
    where invalid_json_count > 0

    union all

    select
        'invalid_last_trade_price',
        'error',
        last_trade_price_issue_count,
        'invalid last-trade price'
    from inventory
    where last_trade_price_issue_count > 0

    union all

    select
        'duplicate_snapshot',
        'error',
        issue_count,
        'duplicate raw snapshot grain'
    from duplicate_snapshots
    where issue_count > 0

    union all

    select
        'duplicate_level',
        'error',
        issue_count,
        'duplicate public mart grain'
    from duplicate_level_grain
    where issue_count > 0

    union all

    select
        'duplicate_side_price',
        'error',
        issue_count,
        'duplicate price within snapshot side'
    from duplicate_side_prices
    where issue_count > 0

    union all

    select
        'level_rank',
        'error',
        issue_count,
        'non-contiguous side-aware level rank'
    from rank_issues
    where issue_count > 0

    union all

    select
        'invalid_level',
        'error',
        numeric_issue_count,
        'invalid price, size, or order count'
    from level_issues
    where numeric_issue_count > 0

    union all

    select
        'cumulative_depth',
        'error',
        cumulative_issue_count,
        'cumulative depth mismatch'
    from level_issues
    where cumulative_issue_count > 0

    union all

    select
        'incomplete_windows',
        'error',
        issue_count,
        'published scan has incomplete leaf windows'
    from window_issues
    where issue_count > 0

    union all

    select
        'empty_book',
        'warning',
        empty_book_count,
        'snapshot contains no bid or ask levels'
    from inventory
    where empty_book_count > 0

    union all

    select
        'crossed_book',
        'warning',
        issue_count,
        'best bid exceeds best ask'
    from crossed_books
    where issue_count > 0

    union all

    select
        'large_snapshot_gap',
        'warning',
        issue_count,
        'token snapshot gap exceeds six hours'
    from snapshot_gaps
    where issue_count > 0
)

select
    issue_key,
    severity,
    affected_rows,
    details,
    (select min(snapshots.scan_id) from snapshots) as scan_id
from issues
