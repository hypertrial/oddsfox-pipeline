{{ config(alias='squad_player_features') }}

select
    squad.source_player_key,
    squad.run_id,
    squad.official_wc2026_squad_team,
    squad.source_team_code,
    squad.official_wc2026_player_name,
    squad.official_wc2026_squad_position,
    squad.official_wc2026_squad_number,
    squad.official_wc2026_squad_club,
    squad.official_wc2026_squad_dob,
    squad.official_wc2026_squad_group,
    squad.official_wc2026_squad_coach,
    squad.official_wc2026_squad_caps,
    squad.official_wc2026_squad_goals,
    player.player_id,
    player.player_name as fifaindex_player_name,
    player.nationality,
    player.overall,
    player.age,
    player.pace,
    player.shooting,
    player.passing_rating,
    player.dribbling,
    player.defending,
    player.physical,
    player.positions,
    player.primary_position,
    player.club,
    player.league,
    player.game_slug,
    player.game_slug as feature_game_slug,
    squad._snapshot_id as squad_snapshot_id,
    player._snapshot_id as player_snapshot_id,
    case
        when player.player_id is null then 'unmatched'
        else 'exact_name_and_team'
    end as official_wc2026_squad_match_quality,
    case when player.player_id is null then 0 else 1 end as candidate_count,
    player.player_id is not null as was_matched_to_fifaindex,
    greatest(squad._collected_at, player._collected_at) as collected_at
from {{ source('wc2026_canonical_raw', 'wikipedia_squads__players') }} as squad
left join {{ source('wc2026_canonical_raw', 'fifaindex__players') }} as player
    on
        player.game_slug = 'fc26'
        and {{ canonical_team_match_key('player.nationality') }}
        = {{ canonical_team_match_key('squad.official_wc2026_squad_team') }}
        and {{ name_match_key('player.player_name') }}
        = {{ name_match_key('squad.official_wc2026_player_name') }}
qualify row_number() over (
    partition by squad.source_player_key
    order by player.overall desc nulls last, player.player_id asc
) = 1
