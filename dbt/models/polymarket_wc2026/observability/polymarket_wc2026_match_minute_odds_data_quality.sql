{{ config(tags=['cross_domain']) }}

with source_inventory as (
    select count(*) as relevant_source_markets
    from {{ ref('stg_polymarket_wc2026_markets') }}
    where
        is_closed
        and sports_market_type in ('moneyline', 'soccer_team_to_advance')
),

international_results_inventory as (
    select
        count(*) as international_results_games,
        count(distinct source_revision) as international_results_revisions,
        count(distinct source_payload_sha256) as international_results_payload_hashes,
        count(*) filter (
            where
            not regexp_full_match(coalesce(source_revision, ''), '[0-9a-f]{40}')
            or not regexp_full_match(
                coalesce(source_payload_sha256, ''), '[0-9a-f]{64}'
            )
        ) as international_results_provenance_issues
    from {{ ref('international_results_wc2026_matches') }}
),

mapped as (
    select
        count(distinct fifa_match_id) as mapped_games,
        count(distinct market_id) as mapped_markets,
        count(distinct market_id) filter (
            where stage = 'group_stage'
        ) as mapped_group_markets,
        count(distinct market_id) filter (
            where stage <> 'group_stage'
        ) as mapped_knockout_markets,
        count(*) filter (
            where fixture_mapping_count <> 1 or primary_mapping_count <> 1
        ) as ambiguous_mappings,
        count(*) filter (
            where game_started_at_utc is null or game_finished_at_utc is null
        ) as missing_timing,
        count(distinct fifa_match_id) filter (
            where fifa_match_id between 1 and 104
        ) as fifa_id_coverage,
        count(distinct fifa_match_id) filter (
            where
            international_results_match_id is not null
            and international_results_mapping_count = 1
        ) as international_results_mapped_games,
        count(distinct international_results_match_id) as international_results_mapped_source_games,
        count(distinct fifa_match_id) filter (
            where international_results_mapping_count <> 1
        ) as international_results_mapping_issues,
        sum(
            date_diff(
                'minute',
                date_trunc('minute', game_started_at_utc),
                date_trunc('minute', game_finished_at_utc)
            ) + 1
        ) as expected_minute_rows
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}
),

mapped_token_ids as (
    select yes_clob_token_id as clob_token_id
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}
    union all
    select no_clob_token_id as clob_token_id
    from {{ ref('int_polymarket_wc2026_match_market_universe') }}
),

mapped_tokens as (
    select count(distinct clob_token_id) as mapped_tokens
    from mapped_token_ids
),

observed_tokens as (
    select count(distinct clob_token_id) as tokens_with_prices
    from {{ ref('int_polymarket_wc2026_match_token_minute_odds') }}
),

candidate as (
    select
        count(*) as actual_minute_rows,
        sum(yes_observed_points + no_observed_points) as observed_prices,
        count(*) filter (where not yes_observed) as yes_null_minutes,
        count(*) filter (where not no_observed) as no_null_minutes,
        count(*) filter (where minute_complete) as complete_minutes,
        count(*) filter (where not is_game_finish_minute) as non_finish_minute_rows,
        count(*) filter (
            where not is_game_finish_minute and minute_complete
        ) as non_finish_complete_minutes,
        count(*) filter (
            where not is_game_start_minute and not is_game_finish_minute
        ) as interior_minute_rows,
        count(*) filter (
            where minute_status = 'interior_incomplete'
        ) as interior_incomplete_minutes,
        count(*) filter (where is_game_finish_minute) as finish_boundary_rows,
        count(*) filter (
            where is_game_finish_minute and not minute_complete
        ) as finish_boundary_incomplete_minutes,
        count(*) filter (where pair_price_anomaly) as pair_price_anomaly_minutes,
        max(yes_no_close_deviation) as max_pair_price_deviation,
        quantile_cont(yes_no_close_deviation, 0.95) as p95_pair_price_deviation,
        count(*) filter (
            where
            (
                yes_observed
                and (
                    yes_open_price not between 0 and 1
                    or yes_high_price not between 0 and 1
                    or yes_low_price not between 0 and 1
                    or yes_close_price not between 0 and 1
                    or yes_average_price not between 0 and 1
                )
            )
            or (
                no_observed
                and (
                    no_open_price not between 0 and 1
                    or no_high_price not between 0 and 1
                    or no_low_price not between 0 and 1
                    or no_close_price not between 0 and 1
                    or no_average_price not between 0 and 1
                )
            )
        ) as invalid_price_rows,
        count(*) filter (
            where
            (
                yes_observed
                and (
                    yes_low_price > yes_high_price
                    or yes_open_price not between yes_low_price and yes_high_price
                    or yes_close_price not between yes_low_price and yes_high_price
                    or yes_average_price not between yes_low_price and yes_high_price
                )
            )
            or (
                no_observed
                and (
                    no_low_price > no_high_price
                    or no_open_price not between no_low_price and no_high_price
                    or no_close_price not between no_low_price and no_high_price
                    or no_average_price not between no_low_price and no_high_price
                )
            )
        ) as invalid_ohlc_rows
    from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
),

latest_fetch_run as (
    select fetch_run_id
    from {{ ref('stg_polymarket_wc2026_match_minute_fetch_audit') }}
    group by fetch_run_id
    order by max(fetch_finished_at) desc, fetch_run_id desc
    limit 1
),

latest_fetch_audit as (
    select
        max(fetch_audit.fetch_run_id) as latest_fetch_run_id,
        count(*) as latest_fetch_audited_tokens,
        count(*) filter (where fetch_audit.fetch_status = 'success')
            as latest_fetch_success_tokens,
        count(*) filter (where fetch_audit.fetch_status = 'empty')
            as latest_fetch_empty_tokens,
        count(*) filter (where fetch_audit.fetch_status = 'error')
            as latest_fetch_error_tokens,
        count(*) filter (where fetch_audit.fetch_status = 'cancelled')
            as latest_fetch_cancelled_tokens,
        count(*) filter (where fetch_audit.raw_published)
            as latest_fetch_published_tokens,
        sum(fetch_audit.source_row_count) as latest_fetch_source_rows,
        sum(fetch_audit.in_game_row_count) as latest_fetch_in_game_rows,
        count(*) filter (
            where
            fetch_audit.fetch_status = 'success'
            and not regexp_full_match(
                coalesce(fetch_audit.in_game_history_sha256, ''), '[0-9a-f]{64}'
            )
        ) as latest_fetch_hash_issues
    from {{ ref('stg_polymarket_wc2026_match_minute_fetch_audit') }} as fetch_audit
    inner join latest_fetch_run as latest
        on fetch_audit.fetch_run_id = latest.fetch_run_id
),

token_coverage as (
    select
        count(*) as token_coverage_rows,
        max(max_observation_gap_seconds) as max_observation_gap_seconds,
        max(first_observation_offset_seconds) as max_first_observation_offset_seconds,
        max(last_observation_offset_seconds) as max_last_observation_offset_seconds,
        min(distinct_price_count) as min_distinct_prices_per_token,
        count(*) filter (where cadence_gap_warning) as cadence_gap_warning_tokens,
        count(*) filter (where first_boundary_offset_warning)
            as first_boundary_offset_warning_tokens,
        count(*) filter (where last_boundary_offset_warning)
            as last_boundary_offset_warning_tokens,
        count(*) filter (where constant_price_warning) as constant_price_warning_tokens
    from {{ ref('polymarket_wc2026_match_minute_token_coverage') }}
),

issues as (
    select
        count(*) filter (where severity = 'warn') as warning_issue_count,
        count(*) filter (where severity = 'error') as error_issue_count,
        count(*) filter (
            where severity = 'error' and issue_type = 'spine'
        ) as elapsed_axis_issue_markets,
        count(*) filter (
            where severity = 'warn' and issue_type = 'timing'
        ) as timing_warning_count
    from {{ ref('polymarket_wc2026_match_minute_odds_quality_issues') }}
),

quality as (
    select
        m.*,
        c.*,
        r.*,
        a.*,
        coverage.*,
        issues.*,
        t.mapped_tokens,
        104 as expected_games,
        248 as expected_markets,
        216 as expected_group_markets,
        32 as expected_knockout_markets,
        496 as expected_tokens,
        s.relevant_source_markets,
        o.tokens_with_prices
    from source_inventory as s
    cross join international_results_inventory as r
    cross join mapped as m
    cross join mapped_tokens as t
    cross join observed_tokens as o
    cross join candidate as c
    cross join latest_fetch_audit as a
    cross join token_coverage as coverage
    cross join issues
)

select
    *,
    case
        when latest_fetch_run_id is null then 'missing'
        when
            latest_fetch_audited_tokens = expected_tokens
            and latest_fetch_success_tokens = expected_tokens
            and latest_fetch_published_tokens = expected_tokens
            and latest_fetch_hash_issues = 0
            then 'published'
        when
            latest_fetch_empty_tokens > 0
            or latest_fetch_error_tokens > 0
            or latest_fetch_cancelled_tokens > 0
            then 'failed'
        when latest_fetch_success_tokens = expected_tokens then 'unpublished'
        else 'partial'
    end as latest_fetch_run_status,
    case
        when actual_minute_rows = 0 then null
        else complete_minutes::double / actual_minute_rows
    end as minute_completeness_ratio,
    case
        when non_finish_minute_rows = 0 then null
        else non_finish_complete_minutes::double / non_finish_minute_rows
    end as non_finish_minute_completeness_ratio,
    case
        when interior_minute_rows = 0 then null
        else
            (interior_minute_rows - interior_incomplete_minutes)::double
            / interior_minute_rows
    end as interior_minute_completeness_ratio,
    nullif(
        concat_ws(
            ',',
            case when relevant_source_markets > 0 and mapped_games <> expected_games then 'game_count' end,
            case when relevant_source_markets > 0 and fifa_id_coverage <> expected_games then 'fifa_id_coverage' end,
            case when relevant_source_markets > 0 and mapped_markets <> expected_markets then 'market_count' end,
            case
                when relevant_source_markets > 0 and mapped_group_markets <> expected_group_markets then 'group_market_count'
            end,
            case
                when
                    relevant_source_markets > 0 and mapped_knockout_markets <> expected_knockout_markets
                    then 'knockout_market_count'
            end,
            case when relevant_source_markets > 0 and mapped_tokens <> expected_tokens then 'token_count' end,
            case when relevant_source_markets > 0 and tokens_with_prices <> expected_tokens then 'token_history' end,
            case
                when
                    relevant_source_markets > 0
                    and (
                        latest_fetch_audited_tokens <> expected_tokens
                        or latest_fetch_success_tokens <> expected_tokens
                        or latest_fetch_published_tokens <> expected_tokens
                        or latest_fetch_hash_issues > 0
                    )
                    then 'fetch_audit'
            end,
            case
                when relevant_source_markets > 0 and international_results_games <> expected_games
                    then 'international_results_game_count'
            end,
            case
                when
                    relevant_source_markets > 0
                    and international_results_mapped_games <> expected_games
                    then 'international_results_mart_coverage'
            end,
            case
                when
                    relevant_source_markets > 0
                    and international_results_mapped_source_games <> expected_games
                    then 'international_results_source_coverage'
            end,
            case
                when international_results_mapping_issues > 0
                    then 'international_results_mapping'
            end,
            case
                when
                    relevant_source_markets > 0
                    and (
                        international_results_revisions <> 1
                        or international_results_payload_hashes <> 1
                        or international_results_provenance_issues > 0
                    )
                    then 'international_results_provenance'
            end,
            case when ambiguous_mappings > 0 then 'ambiguous_mapping' end,
            case when missing_timing > 0 then 'missing_timing' end,
            case when invalid_price_rows > 0 then 'invalid_price' end,
            case when invalid_ohlc_rows > 0 then 'invalid_ohlc' end,
            case when elapsed_axis_issue_markets > 0 then 'elapsed_axis' end,
            case when relevant_source_markets > 0 and actual_minute_rows <> expected_minute_rows then 'minute_spine' end
        ),
        ''
    ) as blocking_issue_keys
from quality
