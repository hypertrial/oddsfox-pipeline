{{ config(alias='player_features') }}

select
    player.game_slug,
    player.competition_key,
    player.player_id,
    player.player_name,
    player.nationality,
    player.positions,
    player.primary_position,
    player.overall,
    player.age,
    player.pace,
    player.shooting,
    player.passing_rating,
    player.dribbling,
    player.defending,
    player.physical,
    player.gk_diving,
    player.gk_handling,
    player.gk_kicking,
    player.gk_positioning,
    player.gk_reflexes,
    player.club,
    player.league,
    player.player_gender,
    player.was_world_cup_squad_member,
    player.world_cup_squad_team,
    player.world_cup_squad_tournament_year,
    squad.official_wc2026_squad_team,
    squad.official_wc2026_squad_position,
    squad.official_wc2026_squad_number,
    squad.official_wc2026_squad_group,
    squad.official_wc2026_squad_coach,
    squad.official_wc2026_squad_caps,
    squad.official_wc2026_squad_goals,
    player._snapshot_id as snapshot_id,
    player._collected_at as collected_at,
    squad.source_player_key is not null as was_official_wc2026_squad_member,
    case
        when squad.source_player_key is null then null
        else 'exact_name_and_team'
    end as official_wc2026_squad_match_quality
from {{ source('wc2026_canonical_raw', 'fifaindex__players') }} as player
left join {{ source('wc2026_canonical_raw', 'wikipedia_squads__players') }} as squad
    on
        {{ canonical_team_match_key('player.nationality') }}
        = {{ canonical_team_match_key('squad.official_wc2026_squad_team') }}
        and {{ name_match_key('player.player_name') }}
        = {{ name_match_key('squad.official_wc2026_player_name') }}
