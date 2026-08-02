{{ config(tags=['wc2026_logical_atlas']) }}
-- costguard: disable-file=SQLCOST020

with latest_observation as (
    select
        event_id,
        max(observed_at) as observed_at
    from {{ ref('stg_polymarket_wc2026_event_snapshots') }}
    group by event_id
),

event_links as (
    select links.*
    from {{ ref('stg_polymarket_wc2026_event_markets') }} as links
    inner join
        latest_observation
        on links.event_id = latest_observation.event_id and links.observed_at = latest_observation.observed_at
    where links.is_enclosing_event
),

catalog_markets as (
    select
        links.event_id,
        markets.market_id,
        markets.sports_market_type,
        markets.group_item_title,
        markets.outcomes,
        try_cast(markets.outcomes as json) as parsed_outcomes,
        coalesce(markets.game_start_time, markets.event_start_time)
            as market_start_at
    from event_links as links
    inner join {{ ref('stg_polymarket_wc2026_event_market_payload_latest') }} as markets
        on links.market_id = markets.market_id
),

moneyline_teams as (
    select distinct
        event_id,
        {{ canonical_team_match_key('group_item_title') }} as team_key
    from catalog_markets
    where
        sports_market_type = 'moneyline'
        and nullif(trim(group_item_title), '') is not null
        and not starts_with(lower(trim(group_item_title)), 'draw')
),

moneyline_pairs as (
    select
        event_id,
        min(team_key) as team_a_key,
        max(team_key) as team_b_key,
        'moneyline_team_pair' as pair_basis
    from moneyline_teams
    group by event_id
    having count(distinct team_key) = 2
),

advance_pairs as (
    select distinct
        event_id,
        least(
            {{ canonical_team_match_key("json_extract_string(parsed_outcomes, '$[0]')") }},
            {{ canonical_team_match_key("json_extract_string(parsed_outcomes, '$[1]')") }}
        ) as team_a_key,
        greatest(
            {{ canonical_team_match_key("json_extract_string(parsed_outcomes, '$[0]')") }},
            {{ canonical_team_match_key("json_extract_string(parsed_outcomes, '$[1]')") }}
        ) as team_b_key,
        'team_to_advance_pair' as pair_basis
    from catalog_markets
    where
        sports_market_type = 'soccer_team_to_advance'
        and parsed_outcomes is not null
        and json_array_length(parsed_outcomes) = 2
        and {{ canonical_team_match_key("json_extract_string(parsed_outcomes, '$[0]')") }}
        <> {{ canonical_team_match_key("json_extract_string(parsed_outcomes, '$[1]')") }}
),

event_pair_candidates as (
    select * from moneyline_pairs
    union all by name
    select * from advance_pairs
),

unique_event_pairs as (
    select
        event_id,
        min(team_a_key) as team_a_key,
        min(team_b_key) as team_b_key,
        min(pair_basis) as pair_basis
    from event_pair_candidates
    group by event_id
    having count(distinct team_a_key || '|' || team_b_key) = 1
),

event_market_times as (
    select
        event_id,
        min(market_start_at) as market_start_at
    from catalog_markets
    where market_start_at is not null
    group by event_id
),

events as (
    select
        events.*,
        coalesce(events.event_start_at, events.start_at, times.market_start_at)
            as fixture_start_at
    from {{ ref('int_polymarket_wc2026_event_latest') }} as events
    left join event_market_times as times on events.event_id = times.event_id
),

exact_score_events as (
    select distinct event_id
    from catalog_markets
    where sports_market_type = 'soccer_exact_score'
),

event_orientations as (
    select
        events.*,
        scores.event_id is not null as has_exact_score_market,
        nullif(
            {{ canonical_team_match_key(
                "regexp_extract(events.event_title,"
                ~ " '(?i)^\\s*(.+?)\\s+(?:vs\\.?|v)\\s+', 1)"
            ) }},
            ''
        ) as event_home_team_key,
        nullif(
            {{ canonical_team_match_key(
                "regexp_extract(events.event_title,"
                ~ " '(?i)(?:vs\\.?|v)\\s+(.+?)(?:\\s+-\\s+(?:exact|correct)"
                ~ " score.*)?\\s*(?:\\?|$)', 1)"
            ) }},
            ''
        ) as event_away_team_key
    from events
    left join exact_score_events as scores on events.event_id = scores.event_id
),

group_fixtures as (
    select
        fifa_match_id,
        stage_key as fixture_stage,
        group_label,
        home_team,
        away_team,
        kickoff_at_utc,
        least(
            {{ canonical_team_match_key('home_team') }},
            {{ canonical_team_match_key('away_team') }}
        ) as team_a_key,
        greatest(
            {{ canonical_team_match_key('home_team') }},
            {{ canonical_team_match_key('away_team') }}
        ) as team_b_key
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }}
    where
        fifa_match_id between 1 and 72
        and stage_key = 'group_stage'
        and group_label between 'a' and 'l'
),

advancement_fixtures as (
    select
        fifa_match_id,
        stage_key as fixture_stage,
        cast(null as varchar) as group_label,
        home_team,
        away_team,
        kickoff_at_utc,
        least(
            {{ canonical_team_match_key('home_team') }},
            {{ canonical_team_match_key('away_team') }}
        ) as team_a_key,
        greatest(
            {{ canonical_team_match_key('home_team') }},
            {{ canonical_team_match_key('away_team') }}
        ) as team_b_key
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }}
    where
        fifa_match_id between 73 and 104
        and nullif(trim(home_team), '') is not null
        and nullif(trim(away_team), '') is not null
),

official_fixtures as (
    select * from group_fixtures
    union all by name
    select * from advancement_fixtures
),

direct_candidates as (
    select
        pairs.event_id,
        fixtures.fifa_match_id,
        fixtures.fixture_stage,
        fixtures.group_label,
        fixtures.home_team,
        fixtures.away_team,
        pairs.pair_basis,
        count(distinct fixtures.fifa_match_id) over (partition by pairs.event_id)
            as fixture_candidate_count
    from unique_event_pairs as pairs
    inner join event_orientations as events on pairs.event_id = events.event_id
    inner join official_fixtures as fixtures
        on
            pairs.team_a_key = fixtures.team_a_key
            and pairs.team_b_key = fixtures.team_b_key
            and events.fixture_start_at is not null
            and abs(epoch(events.fixture_start_at) - epoch(fixtures.kickoff_at_utc))
            <= 86400
            and (
                not events.has_exact_score_market
                or (
                    events.event_home_team_key
                    = {{ canonical_team_match_key('fixtures.home_team') }}
                    and events.event_away_team_key
                    = {{ canonical_team_match_key('fixtures.away_team') }}
                )
            )
),

direct_mappings as (
    select
        event_id,
        fifa_match_id,
        fixture_stage,
        group_label,
        home_team,
        away_team,
        'team_pair_and_official_kickoff:' || pair_basis as fixture_mapping_basis
    from direct_candidates
    where fixture_candidate_count = 1
    qualify row_number() over (partition by event_id order by fifa_match_id) = 1
),

anchor_metadata as (
    select
        mappings.*,
        events.game_id
    from direct_mappings as mappings
    inner join event_orientations as events on mappings.event_id = events.event_id
),

extended_candidates as (
    select
        events.event_id,
        anchors.fifa_match_id,
        anchors.fixture_stage,
        anchors.group_label,
        anchors.home_team,
        anchors.away_team,
        case
            when events.event_id = anchors.event_id
                then anchors.fixture_mapping_basis
            when events.parent_event_id = anchors.event_id
                then 'parent_of_unique_fixture_event'
            else 'shared_game_id_with_unique_fixture_event'
        end as fixture_mapping_basis,
        case
            when events.event_id = anchors.event_id then 1
            when events.parent_event_id = anchors.event_id then 2
            else 3
        end as mapping_preference,
        count(distinct anchors.fifa_match_id) over (partition by events.event_id)
            as fixture_candidate_count
    from event_orientations as events
    inner join anchor_metadata as anchors
        on
            (
                events.event_id = anchors.event_id
                or events.parent_event_id = anchors.event_id
                or (
                    events.game_id is not null
                    and events.game_id = anchors.game_id
                    and (
                        regexp_matches(events.series_slugs_json, '"soccer-fifwc"')
                        or starts_with(lower(events.event_slug), 'fifwc-')
                    )
                )
            )
            and (
                not events.has_exact_score_market
                or (
                    events.event_home_team_key
                    = {{ canonical_team_match_key('anchors.home_team') }}
                    and events.event_away_team_key
                    = {{ canonical_team_match_key('anchors.away_team') }}
                )
            )
)

select
    event_id,
    fifa_match_id,
    fixture_stage,
    group_label,
    home_team,
    away_team,
    fixture_mapping_basis
from extended_candidates
where fixture_candidate_count = 1
qualify row_number() over (
    partition by event_id
    order by mapping_preference, fifa_match_id
) = 1
