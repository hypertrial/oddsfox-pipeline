{{ config(materialized='table', tags=['polygon_settlement']) }}

with seed as (
    select *
    from {{ ref('int_polymarket_wc2026_polygon_settlement_working_set') }}
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
            coalesce(reference_bundle_id, '')
            <> 'bd46a148289f9930da66c140d4d7d2325e95d387'
            or coalesce(reference_table, '') not in (
                '2026--usa/cup.txt',
                '2026--usa/cup_finals.txt'
            )
            or nullif(reference_row_key, '') is null
            or not regexp_full_match(
                coalesce(reference_row_sha256, ''), '[0-9a-f]{64}'
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
                ' ', yes_represents, no_represents, reference_table
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
)

select
    seed_summary.*,
    token_summary.*,
    match_shape_summary.*
from seed_summary
cross join token_summary
cross join match_shape_summary
