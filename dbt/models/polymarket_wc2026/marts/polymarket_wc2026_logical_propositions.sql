{{ config(tags=['wc2026_logical_atlas']) }}

with expanded as (
    select
        markets.*,
        cast(outcome_indexes.outcome_index as integer) as outcome_index,
        json_extract_string(
            markets.parsed_outcomes,
            '$[' || outcome_indexes.outcome_index || ']'
        ) as outcome_label,
        case
            when
                markets.parsed_clob_token_ids is not null
                and json_type(markets.parsed_clob_token_ids) = 'ARRAY'
                and outcome_indexes.outcome_index
                < json_array_length(markets.parsed_clob_token_ids)
                then nullif(trim(json_extract_string(
                    markets.parsed_clob_token_ids,
                    '$[' || outcome_indexes.outcome_index || ']'
                )), '')
        end as clob_token_id
    from {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
    cross join
        unnest(
            range(cast(json_array_length(markets.parsed_outcomes) as bigint))
        ) as outcome_indexes (outcome_index)
    where
        markets.parsed_outcomes is not null
        and json_type(markets.parsed_outcomes) = 'ARRAY'
),

labels as (
    select
        *,
        case
            when market_family = 'team_total'
                then trim(regexp_replace(
                    regexp_extract(
                        group_item_title,
                        '(?i)^(.*?)\s+(?:O/U|Over/Under)',
                        1
                    ),
                    '(?i)\s+(?:1st|first|2nd|second)\s+half$',
                    ''
                ))
            when market_family = 'spread_handicap'
                then regexp_extract(group_item_title, '^(.*?) \(', 1)
            when market_family = 'player_prop'
                then player_subject_label
            when market_family in (
                'stage_reach', 'stage_advance', 'stage_elimination'
            ) then coalesce(stage_subject_label, group_item_title)
            when market_family = 'tournament_statistic'
                then trim(regexp_replace(
                    coalesce(group_item_title, ''),
                    '\s*\([0-9]+\)\s*$',
                    ''
                ))
            else group_item_title
        end as subject_label,
        case
            when lower(outcome_label) in ('yes', 'no', 'over', 'under')
                then coalesce(group_item_title, outcome_label)
            else outcome_label
        end as claim_label,
        try_cast(
            regexp_extract(
                coalesce(outcome_label, '') || ' '
                || coalesce(group_item_title, ''),
                '([0-9]+)\s*-\s*([0-9]+)',
                1
            ) as integer
        ) as score_home,
        try_cast(
            regexp_extract(
                coalesce(outcome_label, '') || ' '
                || coalesce(group_item_title, ''),
                '([0-9]+)\s*-\s*([0-9]+)',
                2
            ) as integer
        ) as score_away,
        case
            when market_neg_risk_market_id is not null
                then market_neg_risk_market_id
            when coalesce(neg_risk, false) and neg_risk_market_id is not null
                then neg_risk_market_id
        end as effective_neg_risk_market_id,
        case
            when
                sports_market_type = 'soccer_player_goals_plus_assists'
                or regexp_matches(
                    lower(coalesce(group_item_title, '') || ' ' || question),
                    'goals?\s*\+\s*assists?|goal contributions'
                ) then 'goal_contributions'
            when
                sports_market_type = 'soccer_player_shots_on_target'
                or regexp_matches(
                    lower(coalesce(group_item_title, '') || ' ' || question),
                    'shots? on target|\bsot\b'
                ) then 'shots_on_target'
            when
                sports_market_type = 'soccer_player_goalkeeper_saves'
                or regexp_matches(
                    lower(coalesce(group_item_title, '') || ' ' || question),
                    '\bsaves?\b'
                ) then 'saves'
            when
                sports_market_type = 'soccer_player_assists'
                or regexp_matches(
                    lower(coalesce(group_item_title, '') || ' ' || question),
                    '\bassists?\b'
                ) then 'assists'
            when
                sports_market_type = 'soccer_player_shots'
                or regexp_matches(
                    lower(coalesce(group_item_title, '') || ' ' || question),
                    '\bshots?\b'
                ) then 'shots'
            when
                sports_market_type = 'soccer_player_goals'
                or regexp_matches(
                    lower(coalesce(group_item_title, '') || ' ' || question),
                    '\bgoals?\b|\bscore\b'
                ) then 'goals'
        end as player_statistic_key,
        market_family = 'player_prop'
        and normalized_threshold is not null
        and outcome_format = 'binary_yes_no'
            as has_player_threshold_semantics,
        market_family = 'tournament_statistic'
        and (
            market_neg_risk_market_id is not null
            or (
                coalesce(neg_risk, false)
                and neg_risk_market_id is not null
            )
        )
        and regexp_matches(
            lower(coalesce(event_title, '')) || ' '
            || lower(coalesce(question, '')),
            'furthest advancing.*(nation|team|country)'
        ) as has_exclusive_selection_semantics,
        regexp_matches(
            lower(coalesce(event_title, '')) || ' '
            || lower(coalesce(question, '')),
            'group of (the )?(world cup )?champion|champion.*origin group'
        ) as has_champion_group_semantics,
        regexp_matches(
            lower(coalesce(event_title, '')) || ' '
            || lower(coalesce(question, '')),
            'highest.?ranking nation eliminated'
        ) as has_highest_ranked_elimination_semantics,
        regexp_matches(
            trim(coalesce(group_item_title, '')),
            '^(?:Player\s+(?:[A-Z]+|[0-9]+)|Country\s+[A-Z]+'
            || '|Team\s+[A-Z]+|[Oo]ther)$'
        ) as has_placeholder_label,
        regexp_matches(
            lower(coalesce(event_title, '') || ' ' || question),
            '(no\.|number) of matches decided by penalty shootout'
        ) as has_penalty_shootout_count_semantics,
        regexp_matches(
            lower(coalesce(event_title, '') || ' ' || question),
            'number of missed penalties'
        ) as has_missed_penalties_semantics,
        regexp_matches(
            lower(coalesce(event_title, '') || ' ' || question),
            'goals h2h'
        ) as has_goals_h2h_semantics,
        regexp_matches(
            lower(coalesce(event_title, '') || ' ' || question),
            'most goal contributions'
        ) as has_most_goal_contributions_semantics,
        regexp_matches(
            lower(coalesce(event_title, '') || ' ' || question),
            'go unbeaten.*group stage|group stage.*go unbeaten'
        ) as has_team_unbeaten_semantics,
        market_family = 'group_qualification'
        and regexp_matches(
            lower(coalesce(event_title, '')) || ' '
            || lower(coalesce(question, '')),
            'group [a-l].*(second|2nd) place'
        ) as has_group_position_semantics,
        regexp_matches(
            lower(coalesce(event_title, '')) || ' '
            || lower(coalesce(question, '')),
            '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
        ) as has_final_matchup_semantics,
        case
            when
                fifa_match_id is null
                and market_family in ('tournament_statistic', 'other_sporting')
                and regexp_matches(
                    lower(coalesce(event_title, '')) || ' '
                    || lower(coalesce(question, '')),
                    'fourth place|4th place'
                ) then 4
            when
                fifa_match_id is null
                and market_family in ('tournament_statistic', 'other_sporting')
                and regexp_matches(
                    lower(coalesce(event_title, '')) || ' '
                    || lower(coalesce(question, '')),
                    'third place|3rd place'
                ) then 3
        end as tournament_position,
        nullif(regexp_extract(
            lower(
                coalesce(group_item_title, '') || ' '
                || coalesce(outcome_label, '')
            ),
            'group ([a-l])',
            1
        ), '') as champion_group_label,
        nullif(regexp_extract(
            lower(coalesce(event_title, '')), 'group ([a-l])', 1
        ), '') as position_group_label,
        nullif(trim(regexp_extract(
            coalesce(group_item_title, ''),
            '(?i)^(.*?)\s+(?:vs\.?|v)\s+',
            1
        )), '') as matchup_team_left_label,
        nullif(trim(regexp_extract(
            coalesce(group_item_title, ''),
            '(?i)(?:vs\.?|v)\s+(.+?)\s*\??$',
            1
        )), '') as matchup_team_right_label,
        case
            when market_family != 'stage_elimination' then target_stage_key
            when regexp_matches(lower(claim_label), 'group stage|group phase')
                then 'group_stage'
            when regexp_matches(lower(claim_label), 'round of 32|round-of-32')
                then 'round_of_32'
            when regexp_matches(lower(claim_label), 'round of 16|round-of-16')
                then 'round_of_16'
            when regexp_matches(lower(claim_label), 'quarter.?final')
                then 'quarterfinal'
            when regexp_matches(lower(claim_label), 'semi.?final')
                then 'semifinal'
            when regexp_matches(lower(claim_label), 'third.?place|3rd place')
                then 'third_place'
            when regexp_matches(lower(claim_label), 'runner.?up|second place|final')
                then 'final'
            when regexp_matches(lower(claim_label), 'champion|winner')
                then 'winner'
            else target_stage_key
        end as proposition_target_stage_key,
        case
            when market_family != 'stage_elimination' then target_stage_rank
            when regexp_matches(lower(claim_label), 'group stage|group phase') then 1
            when regexp_matches(lower(claim_label), 'round of 32|round-of-32') then 2
            when regexp_matches(lower(claim_label), 'round of 16|round-of-16') then 3
            when regexp_matches(lower(claim_label), 'quarter.?final') then 4
            when regexp_matches(lower(claim_label), 'semi.?final') then 5
            when regexp_matches(
                lower(claim_label),
                'third.?place|3rd place|runner.?up|second place|final'
            ) then 6
            when regexp_matches(lower(claim_label), 'champion|winner') then 7
            else target_stage_rank
        end as proposition_target_stage_rank
    from expanded
),

oriented as (
    select
        labels.*,
        subject_team.canonical_team_id as subject_team_id,
        outcome_team.canonical_team_id as outcome_team_id,
        matchup_left_team.canonical_team_id as matchup_left_team_id,
        matchup_right_team.canonical_team_id as matchup_right_team_id,
        coalesce(subject_group.group_label, outcome_group.group_label)
            as canonical_group_label,
        labels.market_family = 'match_result'
        and regexp_matches(
            lower(trim(coalesce(labels.claim_label, ''))),
            '^draw($|\s)'
        ) as is_draw_result,
        case
            when lower(labels.outcome_label) in ('yes', 'over') then 'positive'
            when lower(labels.outcome_label) in ('no', 'under') then 'negative'
            when
                labels.market_family = 'exact_score'
                and labels.score_home is not null
                and labels.score_away is not null then 'positive'
            when
                labels.market_family = 'match_result'
                and regexp_matches(
                    lower(trim(coalesce(labels.claim_label, ''))),
                    '^draw($|\s)'
                ) then 'positive'
            when
                labels.market_family = 'stage_elimination'
                and labels.proposition_target_stage_key is not null
                then 'positive'
            when labels.has_goals_h2h_semantics then 'positive'
            when outcome_team.canonical_team_id is not null then 'positive'
            else 'other'
        end as polarity,
        case
            when
                labels.has_placeholder_label
                and coalesce(
                    subject_team.canonical_team_id,
                    outcome_team.canonical_team_id
                ) is null
                then null
            when
                labels.has_goals_h2h_semantics
                and nullif(trim(labels.outcome_label), '') is not null
                then 'player:' || md5(lower(trim(labels.outcome_label)))
            when
                labels.market_family = 'award_winner'
                and coalesce(
                    subject_team.canonical_team_id, outcome_team.canonical_team_id
                ) is null
                then 'player:' || md5(lower(trim(coalesce(
                    labels.group_item_title, labels.outcome_label
                ))))
            when labels.market_family = 'player_prop'
                then 'player:' || md5(lower(trim(coalesce(
                    labels.subject_label, labels.outcome_label
                ))))
            when
                coalesce(
                    subject_team.canonical_team_id, outcome_team.canonical_team_id
                ) is not null
                then 'team:' || coalesce(
                    outcome_team.canonical_team_id, subject_team.canonical_team_id
                )
        end as candidate_entity_id
    from labels
    left join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as subject_team
        on
            {{ canonical_team_match_key('labels.subject_label') }}
            = subject_team.team_match_key
    left join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as outcome_team
        on
            {{ canonical_team_match_key('labels.outcome_label') }}
            = outcome_team.team_match_key
    left join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as matchup_left_team
        on
            {{ canonical_team_match_key('labels.matchup_team_left_label') }}
            = matchup_left_team.team_match_key
    left join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as matchup_right_team
        on
            {{ canonical_team_match_key('labels.matchup_team_right_label') }}
            = matchup_right_team.team_match_key
    left join {{ ref('int_polymarket_wc2026_logical_team_groups') }} as subject_group
        on subject_team.canonical_team_id = subject_group.canonical_team_id
    left join {{ ref('int_polymarket_wc2026_logical_team_groups') }} as outcome_group
        on outcome_team.canonical_team_id = outcome_group.canonical_team_id
),

semantics as (
    select
        oriented.*,
        case
            when oriented.has_placeholder_label and oriented.candidate_entity_id is null then null
            when oriented.market_family = 'exact_score' then 'exact_score'
            when oriented.market_family = 'team_to_advance' then 'advances_from_match'
            when oriented.market_family = 'match_result' then 'wins_match'
            when oriented.market_family = 'total_goals' then 'total_goals'
            when oriented.market_family = 'team_total' then 'team_total_goals'
            when oriented.market_family = 'both_teams_to_score' then 'both_teams_score'
            when oriented.market_family = 'spread_handicap' then 'covers_spread'
            when oriented.has_champion_group_semantics then 'champion_origin_group'
            when oriented.has_group_position_semantics then 'finishes_group_position'
            when oriented.has_final_matchup_semantics then 'final_matchup'
            when oriented.has_highest_ranked_elimination_semantics
                then 'highest_ranked_group_stage_elimination'
            when oriented.tournament_position is not null
                then 'finishes_tournament_position'
            when oriented.has_goals_h2h_semantics then 'wins_goals_head_to_head'
            when oriented.has_most_goal_contributions_semantics then 'leads_statistic'
            when oriented.has_team_unbeaten_semantics then 'goes_unbeaten'
            when oriented.market_family = 'group_qualification' then 'qualifies_from_group'
            when oriented.market_family = 'tournament_winner' then 'wins_tournament'
            when oriented.market_family in ('stage_reach', 'stage_advance')
                then 'participates_in_stage'
            when
                oriented.market_family = 'stage_elimination'
                and oriented.proposition_target_stage_key = 'winner'
                then 'wins_tournament'
            when oriented.market_family = 'stage_elimination' then 'eliminated_at_stage'
            when oriented.market_family = 'group_winner' then 'wins_group'
            when oriented.market_family = 'award_winner' then 'wins_award'
            when oriented.has_exclusive_selection_semantics
                then 'furthest_advancing_nation'
            when oriented.market_family in ('player_prop', 'tournament_statistic')
                then 'records_statistic'
        end as predicate,
        case
            when oriented.has_goals_h2h_semantics then oriented.candidate_entity_id
            when oriented.has_champion_group_semantics
                then 'tournament:fifa_world_cup_2026'
            when oriented.has_final_matchup_semantics then 'stage:final'
            when oriented.market_family in (
                'exact_score',
                'match_result',
                'total_goals',
                'both_teams_to_score'
            ) and oriented.fifa_match_id is not null then 'fixture:' || oriented.fifa_match_id
            when
                oriented.market_family = 'tournament_statistic'
                and oriented.candidate_entity_id is not null
                then oriented.candidate_entity_id
            when oriented.market_family = 'tournament_statistic'
                then 'tournament:fifa_world_cup_2026'
            else oriented.candidate_entity_id
        end as predicate_subject_entity_id,
        case
            when oriented.has_goals_h2h_semantics
                then 'metric:world_cup_goals_head_to_head'
            when oriented.has_penalty_shootout_count_semantics
                then 'metric:matches_decided_by_penalty_shootout'
            when oriented.has_missed_penalties_semantics
                then 'metric:missed_penalties'
            when
                oriented.market_family = 'player_prop'
                and oriented.player_statistic_key is not null
                then 'metric:' || oriented.player_statistic_key
            when oriented.has_most_goal_contributions_semantics
                then 'metric:goal_contributions'
            when oriented.has_team_unbeaten_semantics then 'stage:group_stage'
            when
                oriented.has_champion_group_semantics
                and oriented.champion_group_label is not null
                then 'group:' || oriented.champion_group_label
            when
                oriented.has_group_position_semantics
                and oriented.position_group_label is not null
                then 'group:' || oriented.position_group_label
            when
                oriented.has_final_matchup_semantics
                and oriented.matchup_left_team_id is not null
                and oriented.matchup_right_team_id is not null
                then
                    'matchup:'
                    || least(oriented.matchup_left_team_id, oriented.matchup_right_team_id)
                    || ':' || greatest(oriented.matchup_left_team_id, oriented.matchup_right_team_id)
            when oriented.has_highest_ranked_elimination_semantics
                then 'stage:group_stage'
            when oriented.tournament_position is not null
                then 'placement:' || oriented.tournament_position
            when
                oriented.market_family = 'exact_score'
                and oriented.score_home is not null and oriented.score_away is not null
                then 'score:' || oriented.score_home || '-' || oriented.score_away
            when oriented.market_family = 'match_result' and oriented.is_draw_result
                then 'result:draw'
            when oriented.market_family = 'match_result' then oriented.candidate_entity_id
            when oriented.market_family in (
                'team_to_advance',
                'total_goals',
                'team_total',
                'both_teams_to_score',
                'spread_handicap'
            ) and oriented.fifa_match_id is not null then 'fixture:' || oriented.fifa_match_id
            when
                oriented.market_family = 'group_qualification'
                and oriented.canonical_group_label is not null
                then 'group:' || oriented.canonical_group_label
            when oriented.market_family = 'tournament_winner'
                then 'tournament:fifa_world_cup_2026'
            when
                oriented.market_family = 'stage_elimination'
                and oriented.proposition_target_stage_key = 'winner'
                then 'tournament:fifa_world_cup_2026'
            when
                oriented.market_family in (
                    'stage_reach', 'stage_advance', 'stage_elimination'
                ) and oriented.proposition_target_stage_key is not null
                then 'stage:' || oriented.proposition_target_stage_key
            when oriented.market_family = 'group_winner'
                then 'group:' || regexp_extract(
                    lower(oriented.event_title), 'group ([a-l])', 1
                )
            when oriented.market_family = 'award_winner' and oriented.canonical_award_id is not null
                then 'award:' || oriented.canonical_award_id
            when oriented.has_exclusive_selection_semantics
                then 'tournament:fifa_world_cup_2026'
            when oriented.market_family in ('player_prop', 'tournament_statistic')
                then 'metric:' || coalesce(oriented.market_slug, oriented.market_id)
        end as predicate_object,
        case
            when oriented.has_final_matchup_semantics then 6
            when oriented.has_highest_ranked_elimination_semantics then 1
            else oriented.proposition_target_stage_rank
        end as predicate_stage_rank,
        case
            when oriented.has_group_position_semantics or oriented.tournament_position is not null
                then 'eq'
            when (
                oriented.has_player_threshold_semantics
                or oriented.has_penalty_shootout_count_semantics
                or oriented.has_missed_penalties_semantics
            ) and lower(oriented.outcome_label) = 'yes' then 'gte'
            when (
                oriented.has_player_threshold_semantics
                or oriented.has_penalty_shootout_count_semantics
                or oriented.has_missed_penalties_semantics
            ) and lower(oriented.outcome_label) = 'no' then 'lt'
            when oriented.market_family in ('exact_score', 'match_result') then 'eq'
            when
                oriented.market_family in ('total_goals', 'team_total')
                and lower(oriented.outcome_label) = 'over' then 'gt'
            when
                oriented.market_family in ('total_goals', 'team_total')
                and lower(oriented.outcome_label) = 'under' then 'lt'
            when oriented.market_family = 'spread_handicap' then 'covers'
        end as operator, -- noqa: RF04
        case
            when (
                oriented.has_player_threshold_semantics
                or oriented.has_penalty_shootout_count_semantics
                or oriented.has_missed_penalties_semantics
            ) and lower(oriented.outcome_label) = 'yes' then oriented.normalized_threshold
            when
                oriented.market_family in ('total_goals', 'team_total')
                and lower(oriented.outcome_label) = 'over' then oriented.normalized_threshold
        end as interval_lower,
        case
            when (
                oriented.has_player_threshold_semantics
                or oriented.has_penalty_shootout_count_semantics
                or oriented.has_missed_penalties_semantics
            ) and lower(oriented.outcome_label) = 'no' then oriented.normalized_threshold
            when
                oriented.market_family in ('total_goals', 'team_total')
                and lower(oriented.outcome_label) = 'under' then oriented.normalized_threshold
        end as interval_upper,
        case
            when (
                oriented.has_player_threshold_semantics
                or oriented.has_penalty_shootout_count_semantics
                or oriented.has_missed_penalties_semantics
            ) and lower(oriented.outcome_label) = 'yes' then true
            when
                oriented.market_family in ('total_goals', 'team_total')
                and lower(oriented.outcome_label) = 'over' then false
        end as interval_lower_inclusive,
        case
            when (
                oriented.has_player_threshold_semantics
                or oriented.has_penalty_shootout_count_semantics
                or oriented.has_missed_penalties_semantics
            ) and lower(oriented.outcome_label) = 'no' then false
            when
                oriented.market_family in ('total_goals', 'team_total')
                and lower(oriented.outcome_label) = 'under' then false
        end as interval_upper_inclusive,
        case
            when oriented.has_group_position_semantics then 2.0
            when oriented.tournament_position is not null
                then cast(oriented.tournament_position as double)
            when
                oriented.has_player_threshold_semantics
                or oriented.has_penalty_shootout_count_semantics
                or oriented.has_missed_penalties_semantics then oriented.normalized_threshold
            when oriented.market_family in (
                'total_goals', 'team_total', 'spread_handicap'
            ) then oriented.normalized_threshold
        end as threshold_value,
        case
            when oriented.market_family = 'spread_handicap' and oriented.outcome_index = 0
                then oriented.normalized_threshold
            when oriented.market_family = 'spread_handicap' then -oriented.normalized_threshold
        end as handicap_value
    from oriented
)

select
    semantics.market_id,
    semantics.condition_id,
    semantics.outcome_index,
    semantics.outcome_label,
    semantics.clob_token_id,
    semantics.market_family,
    semantics.tournament_part,
    semantics.scope_id,
    semantics.resolution_scope,
    semantics.resolution_period,
    semantics.void_semantics,
    semantics.predicate,
    semantics.predicate_subject_entity_id,
    semantics.predicate_object,
    semantics.predicate_stage_rank,
    semantics.proposition_target_stage_key as target_stage_key,
    semantics.polarity,
    semantics.operator,
    semantics.interval_lower,
    semantics.interval_upper,
    semantics.interval_lower_inclusive as lower_inclusive,
    semantics.interval_upper_inclusive as upper_inclusive,
    semantics.threshold_value,
    semantics.threshold_source,
    semantics.handicap_value,
    semantics.score_home,
    semantics.score_away,
    false as event_constraint_complete,
    semantics.logical_usable,
    case
        when semantics.condition_id is not null
            then
                'polymarket:condition:' || semantics.condition_id
                || ':outcome:' || semantics.outcome_index
        else
            'polymarket:market:' || semantics.market_id
            || ':outcome:' || semantics.outcome_index
    end as source_proposition_id,
    coalesce(
        nullif(trim(semantics.question), ''),
        'Polymarket market ' || semantics.market_id
    ) || ' [' || coalesce(
        nullif(trim(semantics.outcome_label), ''),
        'outcome ' || semantics.outcome_index
    ) || ']' as statement, -- noqa: RF04
    'polymarket:market:' || semantics.market_id || ':outcomes'
        as market_constraint_group_id,
    case
        when semantics.void_semantics = 'split_50_50_if_cancelled'
            then 'at_most_one'
        else 'exactly_one'
    end as market_constraint_kind,
    semantics.void_semantics != 'split_50_50_if_cancelled'
        as market_constraint_complete,
    case
        when
            semantics.polarity = 'positive'
            and semantics.outcome_format = 'binary_yes_no'
            and semantics.effective_neg_risk_market_id is not null
            then
                'polymarket:neg-risk-market:'
                || semantics.effective_neg_risk_market_id
                || ':positive-outcomes:' || md5(
                    coalesce(semantics.resolution_scope, '') || '|'
                    || coalesce(semantics.resolution_period, '') || '|'
                    || coalesce(semantics.void_semantics, '')
                )
    end as event_constraint_group_id,
    case
        when
            semantics.polarity = 'positive'
            and semantics.outcome_format = 'binary_yes_no'
            and semantics.effective_neg_risk_market_id is not null
            then 'at_most_one'
    end as event_constraint_kind,
    semantics.logical_usable
    and semantics.predicate is not null
    and semantics.predicate_subject_entity_id is not null
    and semantics.predicate_object is not null
    and not (
        semantics.market_family in (
            'total_goals', 'team_total', 'spread_handicap'
        ) and semantics.threshold_value is null
    )
    and not (
        (
            semantics.has_player_threshold_semantics
            or semantics.has_penalty_shootout_count_semantics
            or semantics.has_missed_penalties_semantics
        ) and semantics.threshold_value is null
    ) as semantic_usable
from semantics
inner join {{ ref('polymarket_wc2026_logical_events') }} as events
    on semantics.primary_event_id = events.event_id
