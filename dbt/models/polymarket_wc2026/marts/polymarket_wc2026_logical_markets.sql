{{ config(tags=['wc2026_logical_atlas']) }}

with bound as (
    select
        markets.*,
        player_teams.canonical_team_entity_id
            as player_national_team_entity_id,
        'team:' || home_team.canonical_team_id as fixture_home_team_entity_id,
        'team:' || away_team.canonical_team_id as fixture_away_team_entity_id
    from {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
    left join
        {{ ref('int_polymarket_wc2026_logical_player_teams') }}
            as player_teams
        on markets.market_subject_entity_id = player_teams.player_entity_id
    left join {{ ref('int_polymarket_wc2026_fixture_events') }} as fixtures
        on markets.primary_event_id = fixtures.event_id
    left join
        {{ ref('int_polymarket_wc2026_logical_team_identities') }}
            as home_team
        on
            home_team.team_match_key
            = {{ canonical_team_match_key('fixtures.home_team') }}
    left join
        {{ ref('int_polymarket_wc2026_logical_team_identities') }}
            as away_team
        on
            away_team.team_match_key
            = {{ canonical_team_match_key('fixtures.away_team') }}
)

select
    market_id,
    condition_id,
    market_slug,
    question,
    description,
    resolution_text,
    resolution_source,
    outcome_format,
    source_url,
    tags_json,
    market_volume_usd_lifetime_reported,
    is_active,
    is_closed,
    is_resolved,
    winning_outcome,
    winning_clob_token_id,
    market_family,
    tournament_part,
    scope_id,
    resolution_scope,
    resolution_period,
    void_semantics,
    sports_market_type,
    group_item_title,
    group_item_threshold,
    line,
    normalized_threshold,
    threshold_source,
    start_at,
    end_at,
    logical_usable,
    outcomes_usable,
    tokens_usable,
    quarantine_reason,
    market_neg_risk_market_id,
    market_neg_risk_request_id,
    market_neg_risk_other,
    primary_event_id,
    fifa_match_id,
    list_sort(list_distinct(list_filter(
        list_value(market_subject_entity_id), entity_id -> entity_id is not null
    ))) as subject_entity_ids,
    list_sort(list_distinct(list_filter(list_value(
        fixture_home_team_entity_id,
        fixture_away_team_entity_id,
        matchup_left_team_entity_id,
        matchup_right_team_entity_id
    ), entity_id -> entity_id is not null))) as participant_entity_ids,
    list_sort(list_distinct(list_filter(
        list_value(player_national_team_entity_id),
        entity_id -> entity_id is not null
    ))) as player_national_team_entity_ids,
    list_sort(list_distinct(list_filter(list_value(
        'tournament:fifa_world_cup_2026',
        case when fifa_match_id is not null then 'fixture:' || fifa_match_id end,
        case
            when fixture_group_label is not null
                then 'group:' || lower(fixture_group_label)
            when starts_with(resolution_scope, 'group:') then resolution_scope
        end,
        case
            when fixture_stage is not null then 'stage:' || fixture_stage
            when target_stage_key is not null and target_stage_key != 'winner'
                then 'stage:' || target_stage_key
            when tournament_part in (
                'group_stage', 'round_of_32', 'round_of_16', 'quarterfinal',
                'semifinal', 'third_place', 'final'
            ) then 'stage:' || tournament_part
        end,
        case
            when canonical_award_id is not null
                then 'award:' || canonical_award_id
        end
    ), entity_id -> entity_id is not null))) as referenced_entity_ids
from bound
