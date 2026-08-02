{{ config(tags=['wc2026_logical_atlas']) }}

with parts (
    tournament_part,
    parent_part,
    stage_rank,
    progression_path,
    progression_branch
) as (
    values
    ('tournament_wide', null, 0, null, null),
    ('group_stage', 'tournament_wide', 1, 'group_stage', 'main'),
    (
        'round_of_32',
        'group_stage',
        2,
        'group_stage/round_of_32',
        'main'
    ),
    (
        'round_of_16',
        'round_of_32',
        3,
        'group_stage/round_of_32/round_of_16',
        'main'
    ),
    (
        'quarterfinal',
        'round_of_16',
        4,
        'group_stage/round_of_32/round_of_16/quarterfinal',
        'main'
    ),
    (
        'semifinal',
        'quarterfinal',
        5,
        'group_stage/round_of_32/round_of_16/quarterfinal/semifinal',
        'main'
    ),
    (
        'third_place',
        'semifinal',
        6,
        'group_stage/round_of_32/round_of_16/quarterfinal/semifinal/third_place',
        'third_place'
    ),
    (
        'final',
        'semifinal',
        6,
        'group_stage/round_of_32/round_of_16/quarterfinal/semifinal/final',
        'championship'
    ),
    ('awards', 'tournament_wide', null, null, null)
),

part_scopes as (
    select
        'scope:wc2026:' || tournament_part as scope_id,
        case
            when parent_part is null then 'scope:wc2026'
            else 'scope:wc2026:' || parent_part
        end as parent_scope_id,
        case
            when tournament_part in ('tournament_wide', 'awards')
                then 'tournament'
            else 'stage'
        end as scope_type,
        tournament_part as scope_key,
        replace(tournament_part, '_', ' ') as display_name,
        stage_rank,
        progression_path,
        progression_branch
    from parts
),

group_scopes as (
    select distinct
        'scope:wc2026:group:' || lower(group_label) as scope_id,
        'scope:wc2026:group_stage' as parent_scope_id,
        'group' as scope_type,
        lower(group_label) as scope_key,
        'Group ' || upper(group_label) as display_name,
        1 as stage_rank,
        'group_stage' as progression_path,
        'main' as progression_branch
    from {{ ref('polymarket_wc2026_logical_entities') }}
    where entity_type = 'group'
),

fixture_scopes as (
    select
        'scope:wc2026:fixture:' || entities.fifa_match_id as scope_id,
        case
            when entities.group_label is not null
                then 'scope:wc2026:group:' || lower(entities.group_label)
            else 'scope:wc2026:' || entities.tournament_part
        end as parent_scope_id,
        'fixture' as scope_type,
        cast(entities.fifa_match_id as varchar) as scope_key,
        entities.display_name,
        parts.stage_rank,
        parts.progression_path,
        parts.progression_branch
    from {{ ref('polymarket_wc2026_logical_entities') }} as entities
    inner join parts on entities.tournament_part = parts.tournament_part
    where entities.entity_type = 'fixture'
),

award_scopes as (
    select
        'scope:wc2026:award:' || entities.canonical_name as scope_id,
        'scope:wc2026:awards' as parent_scope_id,
        'award' as scope_type,
        entities.canonical_name as scope_key,
        entities.display_name,
        cast(null as integer) as stage_rank,
        cast(null as varchar) as progression_path,
        cast(null as varchar) as progression_branch
    from {{ ref('polymarket_wc2026_logical_entities') }} as entities
    where entities.entity_type = 'award'
),

root_scope as (
    select
        'scope:wc2026' as scope_id,
        cast(null as varchar) as parent_scope_id,
        'tournament' as scope_type,
        'fifa_world_cup_2026' as scope_key,
        '2026 FIFA World Cup' as display_name,
        cast(null as integer) as stage_rank,
        cast(null as varchar) as progression_path,
        cast(null as varchar) as progression_branch
)

select * from root_scope
union all by name
select * from part_scopes
union all by name
select * from group_scopes
union all by name
select * from fixture_scopes
union all by name
select * from award_scopes
