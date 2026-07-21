{{ config(materialized='table', tags=['cross_domain']) }}

with source_markets as (
    select *
    from {{ ref('stg_polymarket_wc2026_markets') }}
    where
        is_closed
        and sports_market_type in ('moneyline', 'soccer_team_to_advance')
),

moneyline_markets as (
    select
        *,
        case
            when lower(group_item_title) not like 'draw%'
                then {{ canonical_team_match_key('group_item_title') }}
        end as proposition_team_key,
        case
            when lower(json_extract_string(outcomes, '$[0]')) = 'yes'
                then json_extract_string(clob_token_ids, '$[0]')
            else json_extract_string(clob_token_ids, '$[1]')
        end as yes_clob_token_id,
        case
            when lower(json_extract_string(outcomes, '$[0]')) = 'no'
                then json_extract_string(clob_token_ids, '$[0]')
            else json_extract_string(clob_token_ids, '$[1]')
        end as no_clob_token_id
    from source_markets
    where sports_market_type = 'moneyline'
),

primary_events as (
    select
        event_id,
        event_slug,
        event_title,
        event_start_time,
        event_finished_time,
        event_ended,
        min(proposition_team_key) filter (
            where proposition_team_key is not null
        ) as team_a_key,
        max(proposition_team_key) filter (
            where proposition_team_key is not null
        ) as team_b_key,
        count(*) as moneyline_market_count
    from moneyline_markets
    group by
        event_id,
        event_slug,
        event_title,
        event_start_time,
        event_finished_time,
        event_ended
),

advance_markets as (
    select
        *,
        {{ canonical_team_match_key("json_extract_string(outcomes, '$[0]')") }} as outcome_0_key,
        {{ canonical_team_match_key("json_extract_string(outcomes, '$[1]')") }} as outcome_1_key,
        json_extract_string(clob_token_ids, '$[0]') as outcome_0_token_id,
        json_extract_string(clob_token_ids, '$[1]') as outcome_1_token_id
    from source_markets
    where sports_market_type = 'soccer_team_to_advance'
),

advance_primary_candidates as (
    select
        a.*,
        p.event_id as primary_timing_event_id,
        p.event_slug as primary_timing_event_slug,
        p.event_start_time as game_started_at_utc,
        p.event_finished_time as game_finished_at_utc,
        p.moneyline_market_count,
        count(*) over (partition by a.market_id) as primary_mapping_count
    from advance_markets as a
    inner join primary_events as p
        on
            least(a.outcome_0_key, a.outcome_1_key) = p.team_a_key
            and greatest(a.outcome_0_key, a.outcome_1_key) = p.team_b_key
            and abs(epoch(a.event_start_time) - epoch(p.event_start_time)) <= 60
),

advance_primary as (
    select *
    from advance_primary_candidates
    where primary_mapping_count = 1
),

advance_primary_event_ids as (
    select distinct primary_timing_event_id
    from advance_primary
),

group_primary_events as (
    select p.*
    from primary_events as p
    left join advance_primary_event_ids as knockout
        on p.event_id = knockout.primary_timing_event_id
    where knockout.primary_timing_event_id is null
),

international_results_fixtures as (
    select
        match_id as international_results_match_id,
        match_date,
        home_team,
        away_team,
        source_revision as results_source_revision,
        source_payload_sha256 as results_source_payload_sha256,
        source_loaded_at as results_source_loaded_at,
        {{ canonical_team_match_key('home_team') }} as home_team_key,
        {{ canonical_team_match_key('away_team') }} as away_team_key
    from {{ ref('international_results_wc2026_matches') }}
),

group_schedule_fixtures as (
    select
        match_id as fifa_match_id,
        stage,
        group_label,
        home_team,
        away_team,
        timezone('America/New_York', kickoff_at_et) at time zone 'UTC'
            as kickoff_at_utc,
        {{ canonical_team_match_key('home_team') }} as home_team_key,
        {{ canonical_team_match_key('away_team') }} as away_team_key
    from {{ ref('wc2026_fixtures') }}
    where match_id between 1 and 72
),

group_fixtures as (
    select
        f.fifa_match_id,
        f.stage,
        f.group_label,
        f.kickoff_at_utc,
        r.international_results_match_id,
        r.results_source_revision,
        r.results_source_payload_sha256,
        r.results_source_loaded_at,
        coalesce(r.home_team, f.home_team) as home_team,
        coalesce(r.away_team, f.away_team) as away_team,
        coalesce(r.home_team_key, f.home_team_key) as home_team_key,
        coalesce(r.away_team_key, f.away_team_key) as away_team_key,
        count(r.international_results_match_id) over (
            partition by f.fifa_match_id
        ) as international_results_mapping_count
    from group_schedule_fixtures as f
    left join international_results_fixtures as r
        on
            least(f.home_team_key, f.away_team_key) = least(r.home_team_key, r.away_team_key)
            and greatest(f.home_team_key, f.away_team_key) = greatest(r.home_team_key, r.away_team_key)
            and abs(date_diff('day', r.match_date, cast(f.kickoff_at_utc as date))) <= 1
),

group_market_candidates as (
    select
        f.fifa_match_id,
        'group_stage' as stage, -- noqa: RF04
        f.group_label,
        f.home_team,
        f.away_team,
        f.international_results_match_id,
        f.international_results_mapping_count,
        f.kickoff_at_utc as scheduled_kickoff_at_utc,
        f.results_source_revision,
        f.results_source_payload_sha256,
        f.results_source_loaded_at,
        m.market_id,
        m.condition_id,
        m.question,
        m.sports_market_type,
        m.event_id as selected_market_event_id,
        m.event_slug as selected_market_event_slug,
        p.event_id as primary_timing_event_id,
        p.event_slug as primary_timing_event_slug,
        p.event_start_time as game_started_at_utc,
        p.event_finished_time as game_finished_at_utc,
        m.proposition_team_key,
        f.home_team_key,
        f.away_team_key,
        m.yes_clob_token_id,
        m.no_clob_token_id,
        count(*) over (partition by m.market_id) as fixture_mapping_count
    from moneyline_markets as m
    inner join group_primary_events as p
        on m.event_id = p.event_id
    inner join group_fixtures as f
        on
            p.team_a_key = least(f.home_team_key, f.away_team_key)
            and p.team_b_key = greatest(f.home_team_key, f.away_team_key)
            -- Completed games can move after the published fixture seed; team pair
            -- plus a one-day bound keeps the match unique while using Gamma's actual time.
            and abs(epoch(p.event_start_time) - epoch(f.kickoff_at_utc)) <= 86400
),

group_markets as (
    select
        fifa_match_id,
        stage,
        group_label,
        home_team,
        away_team,
        international_results_match_id,
        international_results_mapping_count,
        scheduled_kickoff_at_utc,
        results_source_revision,
        results_source_payload_sha256,
        results_source_loaded_at,
        market_id,
        condition_id,
        selected_market_event_id,
        selected_market_event_slug,
        primary_timing_event_id,
        primary_timing_event_slug,
        game_started_at_utc,
        game_finished_at_utc,
        sports_market_type,
        case
            when proposition_team_key = home_team_key then 'home_win'
            when proposition_team_key = away_team_key then 'away_win'
            when proposition_team_key is null then 'draw'
        end as proposition_type,
        question,
        case
            when proposition_team_key = home_team_key then home_team || ' wins in regulation'
            when proposition_team_key = away_team_key then away_team || ' wins in regulation'
            else 'match draws in regulation'
        end as yes_represents,
        case
            when proposition_team_key = home_team_key then home_team || ' does not win in regulation'
            when proposition_team_key = away_team_key then away_team || ' does not win in regulation'
            else 'match does not draw in regulation'
        end as no_represents,
        case
            when proposition_team_key = home_team_key then home_team
            when proposition_team_key = away_team_key then away_team
        end as yes_team_name,
        cast(null as varchar) as no_team_name,
        yes_clob_token_id,
        no_clob_token_id,
        fixture_mapping_count,
        1 as primary_mapping_count
    from group_market_candidates
),

knockout_schedule_fixtures as (
    select
        fifa_match_id,
        stage_key as stage, -- noqa: RF04
        home_team,
        away_team,
        kickoff_at_utc,
        {{ canonical_team_match_key('home_team') }} as home_team_key,
        {{ canonical_team_match_key('away_team') }} as away_team_key
    from {{ ref('stg_openfootball_wc2026_knockout_fixtures') }}
    where fifa_match_id between 73 and 104
),

knockout_fixtures as (
    select
        f.fifa_match_id,
        f.stage,
        f.kickoff_at_utc,
        r.international_results_match_id,
        r.results_source_revision,
        r.results_source_payload_sha256,
        r.results_source_loaded_at,
        coalesce(r.home_team, f.home_team) as home_team,
        coalesce(r.away_team, f.away_team) as away_team,
        coalesce(r.home_team_key, f.home_team_key) as home_team_key,
        coalesce(r.away_team_key, f.away_team_key) as away_team_key,
        count(r.international_results_match_id) over (
            partition by f.fifa_match_id
        ) as international_results_mapping_count
    from knockout_schedule_fixtures as f
    left join international_results_fixtures as r
        on
            least(f.home_team_key, f.away_team_key) = least(r.home_team_key, r.away_team_key)
            and greatest(f.home_team_key, f.away_team_key) = greatest(r.home_team_key, r.away_team_key)
            and abs(date_diff('day', r.match_date, cast(f.kickoff_at_utc as date))) <= 1
),

knockout_candidates as (
    select
        a.*,
        f.fifa_match_id,
        f.stage,
        f.home_team,
        f.away_team,
        f.home_team_key,
        f.away_team_key,
        f.international_results_match_id,
        f.international_results_mapping_count,
        f.kickoff_at_utc as scheduled_kickoff_at_utc,
        f.results_source_revision,
        f.results_source_payload_sha256,
        f.results_source_loaded_at,
        count(*) over (partition by a.market_id) as fixture_mapping_count
    from advance_primary as a
    inner join knockout_fixtures as f
        on
            least(a.outcome_0_key, a.outcome_1_key)
            = least(f.home_team_key, f.away_team_key)
            and greatest(a.outcome_0_key, a.outcome_1_key)
            = greatest(f.home_team_key, f.away_team_key)
            and abs(epoch(a.game_started_at_utc) - epoch(f.kickoff_at_utc)) <= 60
),

knockout_markets as (
    select
        fifa_match_id,
        stage,
        cast(null as varchar) as group_label,
        home_team,
        away_team,
        international_results_match_id,
        international_results_mapping_count,
        scheduled_kickoff_at_utc,
        results_source_revision,
        results_source_payload_sha256,
        results_source_loaded_at,
        market_id,
        condition_id,
        event_id as selected_market_event_id,
        event_slug as selected_market_event_slug,
        primary_timing_event_id,
        primary_timing_event_slug,
        game_started_at_utc,
        game_finished_at_utc,
        sports_market_type,
        question,
        home_team as yes_team_name,
        away_team as no_team_name,
        fixture_mapping_count,
        primary_mapping_count,
        case
            when fifa_match_id = 103 then 'home_win_third_place'
            when fifa_match_id = 104 then 'home_wins_final'
            else 'home_advances'
        end as proposition_type,
        case
            when fifa_match_id = 103 then home_team || ' wins the third-place match'
            when fifa_match_id = 104 then home_team || ' wins the final and becomes champion'
            else home_team || ' advances'
        end as yes_represents,
        case
            when fifa_match_id = 103 then away_team || ' wins the third-place match'
            when fifa_match_id = 104 then away_team || ' wins the final and becomes champion'
            else away_team || ' advances'
        end as no_represents,
        case
            when outcome_0_key = home_team_key then outcome_0_token_id
            else outcome_1_token_id
        end as yes_clob_token_id,
        case
            when outcome_0_key = away_team_key then outcome_0_token_id
            else outcome_1_token_id
        end as no_clob_token_id
    from knockout_candidates
)

select * from group_markets
union all by name
select * from knockout_markets
