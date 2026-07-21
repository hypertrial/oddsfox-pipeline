{{ config(tags=['cross_domain']) }}

with source_inventory as (
    select count(*) as relevant_source_markets
    from {{ ref('stg_polymarket_wc2026_markets') }}
    where
        is_closed
        and sports_market_type in ('moneyline', 'soccer_team_to_advance')
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
        count(*) filter (where minute_complete) as complete_minutes
    from {{ ref('int_polymarket_wc2026_match_minute_odds_candidate') }}
),

quality as (
    select
        m.*,
        c.*,
        t.mapped_tokens,
        104 as expected_games,
        248 as expected_markets,
        216 as expected_group_markets,
        32 as expected_knockout_markets,
        496 as expected_tokens,
        s.relevant_source_markets,
        o.tokens_with_prices
    from source_inventory as s
    cross join mapped as m
    cross join mapped_tokens as t
    cross join observed_tokens as o
    cross join candidate as c
)

select
    *,
    case
        when actual_minute_rows = 0 then null
        else complete_minutes::double / actual_minute_rows
    end as minute_completeness_ratio,
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
            case when ambiguous_mappings > 0 then 'ambiguous_mapping' end,
            case when missing_timing > 0 then 'missing_timing' end,
            case when relevant_source_markets > 0 and actual_minute_rows <> expected_minute_rows then 'minute_spine' end
        ),
        ''
    ) as blocking_issue_keys
from quality
