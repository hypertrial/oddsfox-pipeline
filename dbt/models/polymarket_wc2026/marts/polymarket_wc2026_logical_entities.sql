{{ config(tags=['wc2026_logical_atlas']) }}

with logical_events as (
    select *
    from {{ ref('polymarket_wc2026_logical_events') }}
    where fifa_match_id is not null
),

logical_fixtures as (
    select distinct
        events.fifa_match_id,
        events.tournament_part,
        mappings.home_team,
        mappings.away_team,
        lower(mappings.group_label) as group_label,
        'team:' || home_identity.canonical_team_id as home_team_entity_id,
        'team:' || away_identity.canonical_team_id as away_team_entity_id
    from logical_events as events
    inner join {{ ref('int_polymarket_wc2026_fixture_events') }} as mappings
        on events.event_id = mappings.event_id
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as home_identity
        on
            home_identity.team_match_key
            = {{ canonical_team_match_key('mappings.home_team') }}
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as away_identity
        on
            away_identity.team_match_key
            = {{ canonical_team_match_key('mappings.away_team') }}
),

used_subjects as (
    select distinct predicate_subject_entity_id as entity_id
    from {{ ref('polymarket_wc2026_logical_propositions') }}
    where predicate_subject_entity_id is not null

    union

    select distinct market_subject_entity_id
    from {{ ref('int_polymarket_wc2026_logical_markets') }}
    where market_subject_entity_id is not null
),

final_matchup_teams as (
    select distinct 'team:' || identities.canonical_team_id as entity_id
    from {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as identities
        on identities.team_match_key = {{ canonical_team_match_key(
            "regexp_extract(markets.group_item_title, '(?i)^(.*?)\\s+(?:vs\\.?|v)\\s+', 1)"
        ) }}
    where
        regexp_matches(
            lower(coalesce(markets.event_title, '')) || ' '
            || lower(coalesce(markets.question, '')),
            '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
        )

    union

    select distinct 'team:' || identities.canonical_team_id as entity_id
    from {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as identities
        on identities.team_match_key = {{ canonical_team_match_key(
            "regexp_extract(markets.group_item_title, '(?i)(?:vs\\.?|v)\\s+(.+?)\\s*\\??$', 1)"
        ) }}
    where regexp_matches(
        lower(coalesce(markets.event_title, '')) || ' '
        || lower(coalesce(markets.question, '')),
        '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
    )
),

used_teams as (
    select entity_id
    from used_subjects
    where starts_with(entity_id, 'team:')

    union

    select home_team_entity_id from logical_fixtures

    union

    select away_team_entity_id from logical_fixtures

    union

    select canonical_team_entity_id as entity_id
    from {{ ref('int_polymarket_wc2026_logical_player_teams') }}

    union

    select entity_id from final_matchup_teams
),

teams as (
    select
        'team:' || identities.canonical_team_id as entity_id,
        'team' as entity_type,
        min(identities.canonical_match_key) as canonical_name,
        min(identities.team_name) as display_name,
        cast(null as varchar) as tournament_part,
        cast(null as integer) as fifa_match_id,
        cast(null as varchar) as group_label,
        cast(null as varchar) as home_team_entity_id,
        cast(null as varchar) as away_team_entity_id,
        'openfootball_wc2026_team_identities' as source
    from {{ ref('int_polymarket_wc2026_logical_team_identities') }} as identities
    inner join used_teams
        on 'team:' || identities.canonical_team_id = used_teams.entity_id
    group by identities.canonical_team_id
),

player_sources as (
    select
        propositions.predicate_subject_entity_id as entity_id,
        case
            when propositions.market_family = 'award_winner'
                then case
                    when lower(propositions.outcome_label) not in ('yes', 'no')
                        then propositions.outcome_label
                    else markets.group_item_title
                end
            when propositions.predicate = 'wins_goals_head_to_head'
                then propositions.outcome_label
            else markets.player_subject_label
        end as source_player_name,
        propositions.tournament_part,
        mappings.canonical_player_name
    from {{ ref('polymarket_wc2026_logical_propositions') }} as propositions
    inner join {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
        on propositions.market_id = markets.market_id
    left join {{ ref('int_polymarket_wc2026_logical_player_teams') }} as mappings
        on propositions.predicate_subject_entity_id = mappings.player_entity_id
    where starts_with(propositions.predicate_subject_entity_id, 'player:')

    union all

    select
        markets.market_subject_entity_id as entity_id,
        markets.player_subject_label as source_player_name,
        markets.tournament_part,
        mappings.canonical_player_name
    from {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
    left join {{ ref('int_polymarket_wc2026_logical_player_teams') }} as mappings
        on markets.market_subject_entity_id = mappings.player_entity_id
    where
        starts_with(markets.market_subject_entity_id, 'player:')
        and nullif(trim(markets.player_subject_label), '') is not null
),

players as (
    select
        entity_id,
        'player' as entity_type,
        coalesce(
            min(canonical_player_name), min(lower(source_player_name))
        ) as canonical_name,
        coalesce(min(canonical_player_name), min(source_player_name)) as display_name,
        min(tournament_part) as tournament_part,
        cast(null as integer) as fifa_match_id,
        cast(null as varchar) as group_label,
        cast(null as varchar) as home_team_entity_id,
        cast(null as varchar) as away_team_entity_id,
        case
            when min(canonical_player_name) is not null
                then 'wc2026_player_features_unique_name_nationality'
            else 'polymarket_reviewed_market'
        end as source
    from player_sources
    group by entity_id
),

fixtures as (
    select
        'fixture:' || fifa_match_id as entity_id,
        'fixture' as entity_type,
        cast(fifa_match_id as varchar) as canonical_name,
        home_team || ' vs. ' || away_team as display_name,
        tournament_part,
        fifa_match_id,
        group_label,
        home_team_entity_id,
        away_team_entity_id,
        'wc2026_official_fixture_mapping' as source
    from logical_fixtures
),

groups as (
    select distinct
        'group:' || team_groups.group_label as entity_id,
        'group' as entity_type,
        team_groups.group_label as canonical_name,
        'Group ' || upper(team_groups.group_label) as display_name,
        'group_stage' as tournament_part,
        cast(null as integer) as fifa_match_id,
        team_groups.group_label,
        cast(null as varchar) as home_team_entity_id,
        cast(null as varchar) as away_team_entity_id,
        'wc2026_official_fixture_universe' as source
    from {{ ref('int_polymarket_wc2026_logical_team_groups') }} as team_groups
),

stage_references (entity_id) as (
    values
    ('stage:group_stage'),
    ('stage:round_of_32'),
    ('stage:round_of_16'),
    ('stage:quarterfinal'),
    ('stage:semifinal'),
    ('stage:third_place'),
    ('stage:final')
),

stages as (
    select distinct
        stage_refs.entity_id,
        'stage' as entity_type,
        replace(stage_refs.entity_id, 'stage:', '') as canonical_name,
        replace(
            replace(stage_refs.entity_id, 'stage:', ''), '_', ' '
        ) as display_name,
        replace(stage_refs.entity_id, 'stage:', '') as tournament_part,
        cast(null as integer) as fifa_match_id,
        cast(null as varchar) as group_label,
        cast(null as varchar) as home_team_entity_id,
        cast(null as varchar) as away_team_entity_id,
        'wc2026_contract' as source
    from stage_references as stage_refs
    where
        starts_with(stage_refs.entity_id, 'stage:')
        and length(stage_refs.entity_id) > length('stage:')
),

awards as (
    select distinct
        award_ids.entity_id,
        'award' as entity_type,
        replace(award_ids.entity_id, 'award:', '') as canonical_name,
        replace(
            replace(award_ids.entity_id, 'award:', ''), '_', ' '
        ) as display_name,
        'awards' as tournament_part,
        cast(null as integer) as fifa_match_id,
        cast(null as varchar) as group_label,
        cast(null as varchar) as home_team_entity_id,
        cast(null as varchar) as away_team_entity_id,
        'polymarket_reviewed_market' as source
    from (
        select predicate_object as entity_id
        from {{ ref('polymarket_wc2026_logical_propositions') }}
        where starts_with(predicate_object, 'award:')

        union

        select 'award:' || canonical_award_id
        from {{ ref('int_polymarket_wc2026_logical_markets') }}
        where canonical_award_id is not null
    ) as award_ids
),

tournament as (
    select
        'tournament:fifa_world_cup_2026' as entity_id,
        'tournament' as entity_type,
        'fifa_world_cup_2026' as canonical_name,
        '2026 FIFA World Cup' as display_name,
        'tournament_wide' as tournament_part,
        cast(null as integer) as fifa_match_id,
        cast(null as varchar) as group_label,
        cast(null as varchar) as home_team_entity_id,
        cast(null as varchar) as away_team_entity_id,
        'wc2026_contract' as source
)

select * from teams
union all by name
select * from players
union all by name
select * from fixtures
union all by name
select * from groups
union all by name
select * from stages
union all by name
select * from awards
union all by name
select * from tournament
