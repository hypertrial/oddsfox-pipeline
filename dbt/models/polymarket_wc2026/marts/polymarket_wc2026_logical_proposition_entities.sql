{{ config(tags=['wc2026_logical_atlas']) }}

with propositions as (
    select *
    from {{ ref('polymarket_wc2026_logical_propositions') }}
),

subject_links as (
    select
        source_proposition_id,
        predicate_subject_entity_id as entity_id,
        'subject' as entity_role
    from propositions
    where predicate_subject_entity_id is not null
),

referenced_links as (
    select
        source_proposition_id,
        predicate_object as entity_id,
        'referenced' as entity_role
    from propositions
    where
        starts_with(predicate_object, 'fixture:')
        or starts_with(predicate_object, 'team:')
        or starts_with(predicate_object, 'group:')
        or starts_with(predicate_object, 'stage:')
        or starts_with(predicate_object, 'award:')
        or starts_with(predicate_object, 'tournament:')
),

market_context as (
    select distinct
        propositions.source_proposition_id,
        markets.fifa_match_id,
        markets.tournament_part,
        fixtures.group_label
    from propositions
    inner join {{ ref('polymarket_wc2026_logical_markets') }} as markets
        on propositions.market_id = markets.market_id
    left join {{ ref('polymarket_wc2026_logical_entities') }} as fixtures
        on fixtures.entity_id = 'fixture:' || markets.fifa_match_id
),

scope_context_links as (
    select
        source_proposition_id,
        'fixture:' || fifa_match_id as entity_id,
        'referenced' as entity_role
    from market_context
    where fifa_match_id is not null

    union

    select
        source_proposition_id,
        'group:' || lower(group_label) as entity_id,
        'referenced' as entity_role
    from market_context
    where group_label is not null

    union

    select
        source_proposition_id,
        case
            when tournament_part in (
                'group_stage',
                'round_of_32',
                'round_of_16',
                'quarterfinal',
                'semifinal',
                'third_place',
                'final'
            ) then 'stage:' || tournament_part
            else 'tournament:fifa_world_cup_2026'
        end as entity_id,
        'referenced' as entity_role
    from market_context
    where tournament_part is not null
),

fixture_participants as (
    select distinct
        propositions.source_proposition_id,
        entities.home_team_entity_id as entity_id,
        'participant' as entity_role
    from propositions
    inner join {{ ref('polymarket_wc2026_logical_markets') }} as markets
        on propositions.market_id = markets.market_id
    inner join {{ ref('polymarket_wc2026_logical_entities') }} as entities
        on entities.entity_id = 'fixture:' || markets.fifa_match_id
    where
        markets.fifa_match_id is not null
        and entities.home_team_entity_id is not null

    union

    select distinct
        propositions.source_proposition_id,
        entities.away_team_entity_id as entity_id,
        'participant' as entity_role
    from propositions
    inner join {{ ref('polymarket_wc2026_logical_markets') }} as markets
        on propositions.market_id = markets.market_id
    inner join {{ ref('polymarket_wc2026_logical_entities') }} as entities
        on entities.entity_id = 'fixture:' || markets.fifa_match_id
    where
        markets.fifa_match_id is not null
        and entities.away_team_entity_id is not null
),

player_national_teams as (
    select distinct
        propositions.source_proposition_id,
        mappings.canonical_team_entity_id as entity_id,
        'player_national_team' as entity_role
    from propositions
    inner join {{ ref('int_polymarket_wc2026_logical_player_teams') }} as mappings
        on propositions.predicate_subject_entity_id = mappings.player_entity_id
),

final_matchup_participants as (
    select distinct
        propositions.source_proposition_id,
        'team:' || identities.canonical_team_id as entity_id,
        'participant' as entity_role
    from propositions
    inner join {{ ref('polymarket_wc2026_logical_markets') }} as markets
        on propositions.market_id = markets.market_id
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as identities
        on identities.team_match_key = {{ canonical_team_match_key(
            "regexp_extract(markets.group_item_title, '(?i)^(.*?)\\s+(?:vs\\.?|v)\\s+', 1)"
        ) }}
    where propositions.predicate = 'final_matchup'

    union

    select distinct
        propositions.source_proposition_id,
        'team:' || identities.canonical_team_id as entity_id,
        'participant' as entity_role
    from propositions
    inner join {{ ref('polymarket_wc2026_logical_markets') }} as markets
        on propositions.market_id = markets.market_id
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as identities
        on identities.team_match_key = {{ canonical_team_match_key(
            "regexp_extract(markets.group_item_title, '(?i)(?:vs\\.?|v)\\s+(.+?)\\s*\\??$', 1)"
        ) }}
    where propositions.predicate = 'final_matchup'
),

special_stage_references as (
    select
        source_proposition_id,
        'stage:final' as entity_id,
        'referenced' as entity_role
    from propositions
    where predicate = 'final_matchup'

    union all

    select
        source_proposition_id,
        'stage:third_place' as entity_id,
        'referenced' as entity_role
    from propositions
    where predicate = 'finishes_tournament_position'
)

select * from subject_links
union by name
select * from referenced_links
union by name
select * from scope_context_links
union by name
select * from fixture_participants
union by name
select * from player_national_teams
union by name
select * from final_matchup_participants
union by name
select * from special_stage_references
