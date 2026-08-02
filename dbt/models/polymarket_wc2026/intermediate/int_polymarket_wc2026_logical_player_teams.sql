{{ config(tags=['wc2026_logical_atlas']) }}

with proposition_players as (
    select
        propositions.predicate_subject_entity_id as player_entity_id,
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
        end as source_player_name
    from {{ ref('polymarket_wc2026_logical_propositions') }} as propositions
    inner join {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
        on propositions.market_id = markets.market_id
    where starts_with(propositions.predicate_subject_entity_id, 'player:')
),

market_players as (
    select
        markets.market_subject_entity_id as player_entity_id,
        markets.player_subject_label as source_player_name
    from {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
    where
        starts_with(markets.market_subject_entity_id, 'player:')
        and nullif(trim(markets.player_subject_label), '') is not null
),

all_player_sources as (
    select * from proposition_players
    union all
    select * from market_players
),

logical_players as (
    select distinct
        player_entity_id,
        source_player_name,
        {{ name_match_key('source_player_name') }} as player_name_key
    from all_player_sources
    where nullif(trim(source_player_name), '') is not null
),

canonical_players as (
    select distinct
        cast(player_id as varchar) as player_id,
        player_name,
        nationality,
        {{ name_match_key('player_name') }} as player_name_key,
        {{ canonical_team_match_key('nationality') }} as nationality_key
    from {{ ref('wc2026_player_features') }}
    where
        player_id is not null
        and nullif(trim(player_name), '') is not null
        and nullif(trim(nationality), '') is not null
),

candidates as (
    select
        logical.player_entity_id,
        logical.source_player_name,
        players.player_id,
        players.player_name as canonical_player_name,
        players.nationality,
        'team:' || teams.canonical_team_id as canonical_team_entity_id
    from logical_players as logical
    inner join canonical_players as players on logical.player_name_key = players.player_name_key
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as teams
        on players.nationality_key = teams.team_match_key
),

unique_candidates as (
    select
        *,
        count(distinct player_id) over (partition by player_entity_id)
            as player_candidate_count,
        count(distinct canonical_team_entity_id) over (partition by player_entity_id)
            as national_team_candidate_count
    from candidates
)

select
    player_entity_id,
    'wc2026_player_features_unique_name_nationality' as mapping_basis,
    min(player_id) as canonical_player_id,
    min(canonical_player_name) as canonical_player_name,
    min(nationality) as nationality,
    min(canonical_team_entity_id) as canonical_team_entity_id
from unique_candidates
where player_candidate_count = 1 and national_team_candidate_count = 1
group by player_entity_id
