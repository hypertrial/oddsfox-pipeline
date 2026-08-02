{{ config(materialized='table', tags=['wc2026_logical_atlas']) }}
-- costguard: disable-file=SQLCOST036

with primary_links as (
    select
        links.*,
        membership.event_title,
        membership.event_slug,
        membership.event_description,
        membership.resolution_source as event_resolution_source,
        membership.scope_id,
        membership.tournament_part,
        membership.game_id,
        membership.neg_risk as event_neg_risk,
        membership.neg_risk_market_id as event_neg_risk_market_id,
        membership.show_all_outcomes,
        membership.membership_policy_version,
        membership.fixture_group_label,
        membership.fixture_stage
    from {{ ref('int_polymarket_wc2026_logical_market_events') }} as links
    inner join {{ ref('int_polymarket_wc2026_event_membership') }} as membership
        on links.event_id = membership.event_id
    where links.is_primary_qualifying_event
),

team_group_candidates as (
    select
        lower(fixtures.group_label) as group_label,
        fixtures.home_team as team_name
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }} as fixtures
    where fixtures.group_label is not null

    union all

    select
        lower(fixtures.group_label) as group_label,
        fixtures.away_team as team_name
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }} as fixtures
    where fixtures.group_label is not null
),

unique_team_groups as (
    select
        identities.canonical_team_id,
        min(candidates.group_label) as group_label
    from team_group_candidates as candidates
    inner join {{ ref('int_polymarket_wc2026_logical_team_identities') }} as identities
        on
            identities.team_match_key
            = {{ canonical_team_match_key('candidates.team_name') }}
    group by identities.canonical_team_id
    having count(distinct candidates.group_label) = 1
),

base_raw as (
    select
        markets.*,
        primary_links.event_id as primary_event_id,
        primary_links.event_title,
        primary_links.event_slug,
        primary_links.event_description,
        primary_links.event_resolution_source,
        primary_links.scope_id,
        primary_links.tournament_part,
        primary_links.game_id,
        primary_links.fifa_match_id,
        primary_links.fixture_group_label,
        primary_links.fixture_stage,
        primary_links.event_neg_risk,
        primary_links.event_neg_risk_market_id,
        primary_links.show_all_outcomes,
        primary_links.membership_policy_version,
        team_groups.group_label as market_team_group_label,
        lower(coalesce(markets.question, '')) as question_key,
        lower(coalesce(primary_links.event_title, '')) as event_title_key,
        lower(coalesce(markets.tags, '')) as market_tags_key,
        coalesce(markets.description, primary_links.event_description)
            as resolution_text,
        lower(coalesce(markets.description, primary_links.event_description, ''))
            as resolution_text_key,
        try_cast(markets.outcomes as json) as parsed_outcomes,
        try_cast(markets.clob_token_ids as json) as parsed_clob_token_ids,
        coalesce(
            try_cast(regexp_extract(
                lower(coalesce(markets.group_item_title, '')),
                '([0-9]+(?:\.[0-9]+)?)\+\s*'
                || '(?:goals?\s*\+\s*assists?|goals?|assists?|'
                || 'shots?(?:\s+on\s+target)?|sot|saves?|matches?|'
                || 'missed\s+penalties)\b',
                1
            ) as double),
            try_cast(regexp_extract(
                trim(coalesce(markets.group_item_title, '')),
                '^([0-9]+(?:\.[0-9]+)?)\+$',
                1
            ) as double),
            try_cast(regexp_extract(
                lower(coalesce(markets.question, '')),
                '([0-9]+(?:\.[0-9]+)?)\+\s*'
                || '(?:goals?\s*\+\s*assists?|goals?|assists?|'
                || 'shots?(?:\s+on\s+target)?|sot|saves?|matches?|'
                || 'missed\s+penalties)\b',
                1
            ) as double),
            markets.line,
            try_cast(markets.group_item_threshold as double),
            try_cast(regexp_extract(
                lower(coalesce(markets.group_item_title, '')),
                '(o/u|over/under)\s*([0-9]+(?:\.[0-9]+)?)',
                2
            ) as double)
        ) as normalized_threshold,
        case
            when try_cast(regexp_extract(
                lower(coalesce(markets.group_item_title, '')),
                '([0-9]+(?:\.[0-9]+)?)\+\s*'
                || '(?:goals?\s*\+\s*assists?|goals?|assists?|'
                || 'shots?(?:\s+on\s+target)?|sot|saves?|matches?|'
                || 'missed\s+penalties)\b',
                1
            ) as double) is not null then 'group_item_title_plus'
            when try_cast(regexp_extract(
                trim(coalesce(markets.group_item_title, '')),
                '^([0-9]+(?:\.[0-9]+)?)\+$',
                1
            ) as double) is not null then 'group_item_title_plus'
            when try_cast(regexp_extract(
                lower(coalesce(markets.question, '')),
                '([0-9]+(?:\.[0-9]+)?)\+\s*'
                || '(?:goals?\s*\+\s*assists?|goals?|assists?|'
                || 'shots?(?:\s+on\s+target)?|sot|saves?|matches?|'
                || 'missed\s+penalties)\b',
                1
            ) as double) is not null then 'question_plus'
            when markets.line is not null then 'line'
            when try_cast(markets.group_item_threshold as double) is not null
                then 'group_item_threshold'
            when try_cast(regexp_extract(
                lower(coalesce(markets.group_item_title, '')),
                '(o/u|over/under)\s*([0-9]+(?:\.[0-9]+)?)',
                2
            ) as double) is not null then 'group_item_title_ou'
        end as threshold_source
    from {{ ref('stg_polymarket_wc2026_event_market_payload_latest') }} as markets
    inner join primary_links on markets.market_id = primary_links.market_id
    left join
        {{ ref('int_polymarket_wc2026_logical_team_identities') }}
            as market_team_identity
        on
            market_team_identity.team_match_key
            = {{ canonical_team_match_key('markets.group_item_title') }}
    left join unique_team_groups as team_groups
        on market_team_identity.canonical_team_id = team_groups.canonical_team_id
),

base_subjects as (
    select
        *,
        case
            when
                regexp_matches(
                    coalesce(group_item_title, ''),
                    '(?i)^goalscorer:\s*.+$'
                )
                then nullif(trim(regexp_extract(
                    group_item_title, '(?i)^goalscorer:\s*(.+?)\s*$', 1
                )), '')
            when
                regexp_matches(
                    coalesce(group_item_title, ''),
                    '(?i)^.+?:\s*[0-9]+(?:\.[0-9]+)?\+?\s*'
                    || '(?:goals?\s*\+\s*assists?|goals?|assists?|'
                    || 'shots?(?:\s+on\s+target)?|sot|saves?)\s*$'
                )
                then nullif(trim(regexp_extract(
                    group_item_title, '^(.*?):', 1
                )), '')
            when
                nullif(trim(coalesce(group_item_title, '')), '') is not null
                and not regexp_matches(
                    trim(group_item_title), '^[1-9](?:\.[0-9]+)?\+?$'
                ) and sports_market_type like 'soccer_player_%'
                then trim(group_item_title)
            when
                regexp_matches(
                    event_title_key || ' ' || market_tags_key,
                    'golden boot|award winner|golden ball|silver ball|bronze ball|'
                    || 'golden glove|fair play award|young player award'
                ) and nullif(trim(coalesce(group_item_title, '')), '') is not null
                then trim(group_item_title)
            when
                nullif(trim(regexp_extract(
                    coalesce(question, ''),
                    '(?i)^(?:will\s+)?(.+?)\s+to\s+(?:score|play)\b',
                    1
                )), '') is not null
                then trim(regexp_extract(
                    question,
                    '(?i)^(?:will\s+)?(.+?)\s+to\s+(?:score|play)\b',
                    1
                ))
            when
                nullif(trim(regexp_extract(
                    coalesce(question, ''),
                    '(?i)^will\s+(.+?)\s+(?:score|record|make|have|provide|assist|take|save)\b',
                    1
                )), '') is not null
                then trim(regexp_extract(
                    question,
                    '(?i)^will\s+(.+?)\s+(?:score|record|make|have|provide|assist|take|save)\b',
                    1
                ))
            when regexp_matches(
                trim(coalesce(group_item_title, '')),
                '^[1-9](?:\.[0-9]+)?\+?$'
            ) then nullif(trim(regexp_extract(
                coalesce(event_title, ''),
                '(?i)^(.*?)\s+(?:goals?|assists?|goals?\s*\+\s*assists?|'
                || 'shots?(?:\s+on\s+target)?|sot|saves?)\??$',
                1
            )), '')
        end as player_subject_label
    from base_raw
),

base as (
    select
        * exclude (player_subject_label),
        case
            when regexp_matches(
                trim(coalesce(player_subject_label, '')),
                '^(?:Player\s+(?:[A-Z]+|[0-9]+)|[Oo]ther)$'
            ) then null
            else nullif(trim(regexp_replace(
                coalesce(player_subject_label, ''),
                '(?i)^world cup:\s*',
                ''
            )), '')
        end as player_subject_label
    from base_subjects
),

classified as (
    select
        *,
        case
            when (
                neg_risk_market_id is not null
                or (
                    coalesce(event_neg_risk, false)
                    and event_neg_risk_market_id is not null
                )
            )
            and regexp_matches(
                event_title_key || ' ' || question_key,
                'furthest advancing.*(nation|team|country)'
            ) then 'tournament_statistic'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                '(nation|country|team).*(top goal.?scorer|top scorer)|'
                || '(top goal.?scorer|top scorer).*(nation|country|team)'
            ) then 'tournament_statistic'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                '(no\.|number) of matches decided by penalty shootout|'
                || 'number of missed penalties|goals h2h'
            ) then 'tournament_statistic'
            when sports_market_type = 'soccer_exact_score'
                then 'exact_score'
            when sports_market_type = 'soccer_team_to_advance'
                then 'team_to_advance'
            when sports_market_type in (
                'moneyline', 'soccer_halftime_result', 'soccer_second_half_result'
            ) then 'match_result'
            when sports_market_type in (
                'totals', 'first_half_totals', 'second_half_totals'
            ) and regexp_matches(
                question_key || ' ' || resolution_text_key,
                '\bgoals?\b'
            ) then 'total_goals'
            when sports_market_type in (
                'soccer_team_totals',
                'soccer_first_half_team_totals',
                'soccer_second_half_team_totals'
            ) and regexp_matches(
                question_key || ' ' || resolution_text_key,
                '\bgoals?\b'
            ) then 'team_total'
            when sports_market_type in (
                'both_teams_to_score',
                'both_teams_to_score_first_half',
                'both_teams_to_score_second_half'
            ) then 'both_teams_to_score'
            when sports_market_type = 'spreads' then 'spread_handicap'
            when sports_market_type in (
                'total_corners',
                'first_half_total_corners',
                'second_half_total_corners',
                'soccer_team_total_corners',
                'soccer_first_half_team_total_corners',
                'soccer_second_half_team_total_corners',
                'total_cards',
                'total_shots'
            ) then 'other_sporting'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'group [a-l].*(second|2nd) place'
            ) then 'group_qualification'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'team to advance to knockout|qualif(y|ies) from group|advance from group'
            ) then 'group_qualification'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'advancing group stage third.?place team|third.?place teams? to advance'
            ) then 'group_qualification'
            when regexp_matches(
                event_title_key || ' ' || market_tags_key,
                'golden boot|award winner|golden ball|silver ball|bronze ball|golden glove|fair play award|young player award'
            ) then 'award_winner'
            when regexp_matches(
                question_key || ' ' || event_title_key,
                'world cup winner|win the 2026 fifa world cup'
            ) then 'tournament_winner'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'highest.?ranking nation eliminated'
            ) then 'tournament_statistic'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'stage of elimination|eliminated'
            ) then 'stage_elimination'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'reach (the )?(world cup )?(round|quarter|semi|final)'
            ) then 'stage_reach'
            when regexp_matches(event_title_key || ' ' || question_key, 'advance to')
                then 'stage_advance'
            when regexp_matches(event_title_key, 'group [a-l] winner')
                then 'group_winner'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
            ) then 'other_sporting'
            when
                player_subject_label is not null
                and (
                    sports_market_type like 'soccer_player_%'
                    or regexp_matches(
                        event_title_key || ' ' || question_key || ' ' || market_tags_key,
                        'goalscorer|\bscore\b|to play|assists?|shots?|saves?|'
                        || 'goal contributions'
                    )
                )
                then 'player_prop'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'go unbeaten.*group stage|group stage.*go unbeaten'
            ) then 'tournament_statistic'
            when regexp_matches(
                event_title_key || ' ' || question_key || ' ' || market_tags_key,
                'record|number of|most |highest |total |how many|'
                || 'goals? (?:will|in|at)|group of (the )?champion|'
                || '(third|3rd|fourth|4th) place'
            ) then 'tournament_statistic'
            when regexp_matches(
                event_title_key || ' ' || market_tags_key,
                'team|nation|unbeaten|concede'
            ) then 'other_sporting'
            when
                fifa_match_id is not null
                or regexp_matches(market_tags_key, 'soccer|fifa-world-cup')
                then 'other_sporting'
            else 'unclassified'
        end as market_family,
        case
            when fifa_match_id is not null then 'fixture:' || fifa_match_id
            when
                regexp_matches(
                    event_title_key || ' ' || question_key,
                    'team to advance to knockout|qualif(y|ies) from group|'
                    || 'advance from group|advancing group stage third.?place team|'
                    || 'third.?place teams? to advance'
                ) and market_team_group_label is not null
                then 'group:' || market_team_group_label
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'go unbeaten.*group stage|group stage.*go unbeaten'
            ) then 'stage:group_stage'
            when regexp_matches(event_title_key, 'group [a-l]')
                then 'group:' || regexp_extract(event_title_key, 'group ([a-l])', 1)
            else 'tournament:fifa_world_cup_2026'
        end as resolution_scope,
        case
            when
                sports_market_type like '%first_half%'
                or sports_market_type = 'soccer_halftime_result'
                then 'first_half_regulation'
            when
                sports_market_type like '%second_half%'
                or sports_market_type = 'soccer_second_half_result'
                then 'second_half_regulation'
            when sports_market_type = 'soccer_team_to_advance'
                then 'full_match_including_extra_time_and_penalties'
            when fifa_match_id is not null then 'regulation_90_plus_stoppage'
            else 'tournament'
        end as resolution_period,
        case
            when regexp_matches(resolution_text_key, 'resolve(s|d)? (to )?50.?50')
                then 'split_50_50_if_cancelled'
            when regexp_matches(resolution_text_key, 'cancell?ed[^.]*resolve(s|d)? (to )?["“]?0-0')
                then 'score_0_0_if_cancelled'
            when regexp_matches(resolution_text_key, 'cancell?ed[^.]*resolve(s|d)? (to )?["“]?no')
                then 'no_if_cancelled'
            when
                regexp_matches(resolution_text_key, 'cancell?ed|postponed')
                and regexp_matches(resolution_text_key, 'resolve(s|d)? (to )?["“]?other')
                then 'other_outcome_if_cancelled'
            else 'unspecified'
        end as void_semantics,
        case
            when regexp_matches(event_title_key, 'golden boot') then 'golden_boot'
            when regexp_matches(event_title_key, 'young player') then 'young_player'
            when regexp_matches(event_title_key, 'bronze ball') then 'bronze_ball'
            when regexp_matches(event_title_key, 'silver ball') then 'silver_ball'
            when regexp_matches(event_title_key, 'golden ball') then 'golden_ball'
            when regexp_matches(event_title_key, 'golden glove') then 'golden_glove'
            when regexp_matches(event_title_key, 'fair play') then 'fair_play'
        end as canonical_award_id
    from base
),

stage_classified as (
    select
        *,
        case
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'round of 32|round-of-32'
            ) then 'round_of_32'
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'round of 16|round-of-16'
            ) then 'round_of_16'
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'quarter.?final'
            ) then 'quarterfinal'
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'semi.?final'
            ) then 'semifinal'
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'third.?place|3rd place'
            ) then 'third_place'
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                '\bfinal\b'
            ) then 'final'
        end as target_stage_key,
        case
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'round of 32|round-of-32'
            ) then 2
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'round of 16|round-of-16'
            ) then 3
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'quarter.?final'
            ) then 4
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'semi.?final'
            ) then 5
            when regexp_matches(
                question_key || ' ' || lower(coalesce(group_item_title, '')),
                'third.?place|3rd place|\bfinal\b'
            ) then 6
        end as target_stage_rank,
        coalesce(
            nullif(
                trim(regexp_extract(
                    coalesce(question, ''),
                    '(?i)^will (.*?) (?:reach|advance|be eliminated)',
                    1
                )),
                ''
            ),
            nullif(
                trim(regexp_extract(
                    coalesce(question, ''),
                    '(?i)^(?:at what|which) stage will (.*?) be eliminated',
                    1
                )),
                ''
            ),
            nullif(
                trim(regexp_extract(
                    coalesce(event_title, ''),
                    '(?i)^(?:world cup:\s*)?(.*?)\s+stage of elimination',
                    1
                )),
                ''
            ),
            nullif(
                trim(regexp_extract(
                    coalesce(question, ''),
                    '(?i)^will\s+(.*?)\s+win\s+the\s+'
                    || '(?:2026\s+fifa\s+)?world cup',
                    1
                )),
                ''
            )
        ) as stage_subject_label
    from classified
),

market_scoped as (
    select
        * exclude (tournament_part, scope_id),
        case
            when fifa_match_id is not null then tournament_part
            when regexp_matches(
                event_title_key || ' ' || question_key,
                '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
            ) then 'final'
            when market_family = 'award_winner' then 'awards'
            when market_family in (
                'stage_reach', 'stage_advance', 'stage_elimination'
            ) and target_stage_key is not null then target_stage_key
            when market_family in ('group_winner', 'group_qualification')
                then 'group_stage'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'go unbeaten.*group stage|group stage.*go unbeaten'
            ) then 'group_stage'
            when market_family in ('tournament_winner', 'tournament_statistic')
                then 'tournament_wide'
            else tournament_part
        end as tournament_part,
        case
            when fifa_match_id is not null then scope_id
            when regexp_matches(
                event_title_key || ' ' || question_key,
                '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
            ) then 'scope:wc2026:final'
            when market_family = 'award_winner' and canonical_award_id is not null
                then 'scope:wc2026:award:' || canonical_award_id
            when
                market_family in (
                    'stage_reach', 'stage_advance', 'stage_elimination'
                ) and target_stage_key is not null
                then 'scope:wc2026:' || target_stage_key
            when
                market_family in ('group_winner', 'group_qualification')
                and coalesce(
                    market_team_group_label,
                    nullif(regexp_extract(event_title_key, 'group ([a-l])', 1), '')
                ) is not null
                then
                    'scope:wc2026:group:'
                    || coalesce(
                        market_team_group_label,
                        regexp_extract(event_title_key, 'group ([a-l])', 1)
                    )
            when market_family in ('group_winner', 'group_qualification')
                then 'scope:wc2026:group_stage'
            when regexp_matches(
                event_title_key || ' ' || question_key,
                'go unbeaten.*group stage|group stage.*go unbeaten'
            ) then 'scope:wc2026:group_stage'
            when market_family in ('tournament_winner', 'tournament_statistic')
                then 'scope:wc2026:tournament_wide'
            else scope_id
        end as scope_id
    from stage_classified
),

normalization_counts as (
    select
        *,
        (
            select count(*)
            from
                unnest(
                    range(cast(json_array_length(parsed_outcomes) as bigint))
                ) as outcome_indexes (outcome_index)
            where nullif(
                trim(json_extract_string(
                    parsed_outcomes,
                    '$[' || outcome_indexes.outcome_index || ']'
                )),
                ''
            ) is not null
        ) as nonempty_outcome_count,
        (
            select count(*)
            from
                unnest(
                    range(cast(json_array_length(parsed_clob_token_ids) as bigint))
                ) as token_indexes (token_index)
            where nullif(
                trim(json_extract_string(
                    parsed_clob_token_ids,
                    '$[' || token_indexes.token_index || ']'
                )),
                ''
            ) is not null
        ) as nonempty_token_count
    from market_scoped
),

normalization_checks as (
    select
        *,
        parsed_outcomes is not null
        and json_array_length(parsed_outcomes) >= 1
        and nonempty_outcome_count = json_array_length(parsed_outcomes)
            as outcomes_usable,
        coalesce(
            parsed_clob_token_ids is not null
            and json_array_length(parsed_clob_token_ids) >= 1
            and json_array_length(parsed_outcomes)
            = json_array_length(parsed_clob_token_ids)
            and nonempty_token_count = json_array_length(parsed_clob_token_ids),
            false
        ) as tokens_usable
    from normalization_counts
),

validated as (
    select
        *,
        nullif(trim(question), '') is not null
        and outcomes_usable
        and json_array_length(parsed_outcomes) >= 2
        and tokens_usable
        and not (
            market_family in ('total_goals', 'team_total', 'spread_handicap')
            and normalized_threshold is null
        ) as logical_usable
    from normalization_checks
),

subject_bound as (
    select
        validated.*,
        case
            when subject_team.canonical_team_id is not null
                then 'team:' || subject_team.canonical_team_id
            when
                validated.market_family in ('player_prop', 'award_winner')
                and validated.player_subject_label is not null
                then 'player:' || md5(lower(trim(validated.player_subject_label)))
        end as market_subject_entity_id,
        case
            when
                regexp_matches(
                    validated.event_title_key || ' ' || validated.question_key,
                    '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
                ) and matchup_left_team.canonical_team_id is not null
                then 'team:' || matchup_left_team.canonical_team_id
        end as matchup_left_team_entity_id,
        case
            when
                regexp_matches(
                    validated.event_title_key || ' ' || validated.question_key,
                    '(finals?|final) exact match.?up|exact match.?up.*(finals?|final)'
                ) and matchup_right_team.canonical_team_id is not null
                then 'team:' || matchup_right_team.canonical_team_id
        end as matchup_right_team_entity_id
    from validated
    left join
        {{ ref('int_polymarket_wc2026_logical_team_identities') }}
            as subject_team
        on subject_team.team_match_key = {{ canonical_team_match_key(
            'coalesce(stage_subject_label, player_subject_label, group_item_title)'
        ) }}
    left join
        {{ ref('int_polymarket_wc2026_logical_team_identities') }}
            as matchup_left_team
        on matchup_left_team.team_match_key = {{ canonical_team_match_key(
            "regexp_extract(group_item_title, '(?i)^(.*?)\\s+(?:vs\\.?|v)\\s+', 1)"
        ) }}
    left join
        {{ ref('int_polymarket_wc2026_logical_team_identities') }}
            as matchup_right_team
        on matchup_right_team.team_match_key = {{ canonical_team_match_key(
            "regexp_extract(group_item_title, '(?i)(?:vs\\.?|v)\\s+(.+?)\\s*\\??$', 1)"
        ) }}
)

select
    market_id,
    condition_id,
    slug as market_slug,
    question,
    description,
    resolution_text,
    tags as tags_json,
    volume as market_volume_usd_lifetime_reported,
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
    canonical_award_id,
    target_stage_key,
    target_stage_rank,
    stage_subject_label,
    player_subject_label,
    sports_market_type,
    group_item_title,
    group_item_threshold,
    line,
    normalized_threshold,
    threshold_source,
    end_date as end_at,
    logical_usable,
    outcomes_usable,
    tokens_usable,
    primary_event_id,
    event_title,
    game_id,
    fifa_match_id,
    fixture_group_label,
    fixture_stage,
    event_neg_risk as neg_risk,
    event_neg_risk_market_id as neg_risk_market_id,
    neg_risk_market_id as market_neg_risk_market_id,
    neg_risk_request_id as market_neg_risk_request_id,
    neg_risk_other as market_neg_risk_other,
    show_all_outcomes,
    membership_policy_version,
    market_subject_entity_id,
    matchup_left_team_entity_id,
    matchup_right_team_entity_id,
    outcomes,
    clob_token_ids,
    parsed_outcomes,
    parsed_clob_token_ids,
    coalesce(market_resolution_source, event_resolution_source)
        as resolution_source,
    case
        when
            json_array_length(parsed_outcomes) = 2
            and lower(json_extract_string(parsed_outcomes, '$[0]')) = 'yes'
            and lower(json_extract_string(parsed_outcomes, '$[1]')) = 'no'
            then 'binary_yes_no'
        when sports_market_type in (
            'totals',
            'first_half_totals',
            'second_half_totals',
            'soccer_team_totals',
            'soccer_first_half_team_totals',
            'soccer_second_half_team_totals'
        ) then 'over_under'
        else 'categorical'
    end as outcome_format,
    'https://polymarket.com/event/' || event_slug as source_url,
    coalesce(game_start_time, event_start_time, created_at) as start_at,
    case
        when nullif(trim(question), '') is null then 'missing_question'
        when parsed_outcomes is null then 'invalid_outcomes_json'
        when json_array_length(parsed_outcomes) < 2 then 'insufficient_outcomes'
        when not outcomes_usable then 'empty_outcome_label'
        when parsed_clob_token_ids is null then 'invalid_token_ids_json'
        when
            json_array_length(parsed_outcomes)
            != json_array_length(parsed_clob_token_ids)
            then 'outcome_token_cardinality_mismatch'
        when not tokens_usable then 'empty_token_id'
        when
            market_family in ('total_goals', 'team_total', 'spread_handicap')
            and normalized_threshold is null then 'missing_numeric_threshold'
    end as quarantine_reason
from subject_bound
