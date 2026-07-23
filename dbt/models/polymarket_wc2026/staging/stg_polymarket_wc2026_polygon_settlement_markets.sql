{{ config(tags=['polygon_settlement']) }}

select  -- noqa: ST06
    cast(proposition_id as varchar) as proposition_id,
    cast(fifa_match_id as integer) as fifa_match_id,
    cast(stage as varchar) as stage,  -- noqa: RF04
    nullif(cast(group_label as varchar), '') as group_name,
    cast(home_team as varchar) as home_team,
    cast(away_team as varchar) as away_team,
    cast(kickoff_at_utc as timestamp) as scheduled_kickoff_at_utc,
    cast(window_start_at_utc as timestamp) as analysis_window_start_at_utc,
    cast(window_end_at_utc as timestamp) as analysis_window_end_at_utc,
    cast(proposition_type as varchar) as proposition_type,
    cast(yes_represents as varchar) as yes_represents,
    cast(no_represents as varchar) as no_represents,
    lower(cast(condition_id as varchar)) as condition_id,
    cast(yes_token_id as varchar) as yes_token_id,
    cast(no_token_id as varchar) as no_token_id,
    cast(market_structure as varchar) as market_structure,
    lower(cast(exchange_address as varchar)) as exchange_address,
    lower(cast(openfootball_revision as varchar)) as openfootball_revision,
    cast(openfootball_path as varchar) as openfootball_path,
    cast(openfootball_source_lines as varchar) as openfootball_source_lines,
    lower(cast(openfootball_line_hash as varchar)) as openfootball_line_hash,
    lower(cast(condition_init_tx_hash as varchar)) as condition_init_tx_hash,
    cast(condition_init_log_index as bigint) as condition_init_log_index,
    lower(cast(question_init_tx_hash as varchar)) as question_init_tx_hash,
    cast(question_init_log_index as bigint) as question_init_log_index,
    lower(cast(ancillary_data_sha256 as varchar)) as ancillary_data_sha256,
    cast(token_verification_block_number as bigint)
        as token_verification_block_number,
    lower(cast(token_verification_block_hash as varchar))
        as token_verification_block_hash,
    lower(cast(manifest_sha256 as varchar)) as manifest_sha256,
    cast(manifest_version as varchar) as manifest_version,
    cast(reviewed_at_utc as timestamp) as reviewed_at_utc
from {{ ref('polymarket_wc2026_polygon_settlement_markets') }}
