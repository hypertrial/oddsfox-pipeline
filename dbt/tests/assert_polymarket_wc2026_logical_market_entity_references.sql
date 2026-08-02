{{ config(
    tags=['wc2026_logical_atlas'],
    meta={
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_logical_markets'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'logical_markets']
        }
    }
) }}

with bindings as (
    select
        market_id,
        'subject' as entity_role,
        unnest(subject_entity_ids) as entity_id
    from {{ ref('polymarket_wc2026_logical_markets') }}

    union all

    select
        market_id,
        'participant' as entity_role,
        unnest(participant_entity_ids) as entity_id
    from {{ ref('polymarket_wc2026_logical_markets') }}

    union all

    select
        market_id,
        'player_national_team' as entity_role,
        unnest(player_national_team_entity_ids) as entity_id
    from {{ ref('polymarket_wc2026_logical_markets') }}

    union all

    select
        market_id,
        'referenced' as entity_role,
        unnest(referenced_entity_ids) as entity_id
    from {{ ref('polymarket_wc2026_logical_markets') }}
)

select bindings.*
from bindings
left join {{ ref('polymarket_wc2026_logical_entities') }} as entities
    on bindings.entity_id = entities.entity_id
where
    entities.entity_id is null
    or (
        bindings.entity_role = 'subject'
        and entities.entity_type not in ('team', 'player')
    )
    or (
        bindings.entity_role in ('participant', 'player_national_team')
        and entities.entity_type != 'team'
    )
    or (
        bindings.entity_role = 'referenced'
        and entities.entity_type not in (
            'fixture', 'group', 'stage', 'award', 'tournament'
        )
    )
