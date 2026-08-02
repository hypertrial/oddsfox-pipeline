{{ config(tags=['wc2026_logical_atlas']) }}

with contract as (
    select *
    from {{ ref('polymarket_wc2026_logical_contract') }}
),

events as (
    select
        *,
        lower(coalesce(event_title, '')) as title_key,
        lower(coalesce(event_slug, '')) as slug_key,
        lower(coalesce(tags_json, '[]')) as tags_key,
        lower(coalesce(series_slugs_json, '[]')) as series_key
    from {{ ref('int_polymarket_wc2026_event_latest') }}
),

fixtures as (
    select *
    from {{ ref('int_polymarket_wc2026_fixture_events') }}
),

overrides as (
    select
        cast(event_id as varchar) as event_id,
        cast(membership_status as varchar) as membership_status,
        cast(membership_class as varchar) as membership_class,
        cast(tournament_part as varchar) as tournament_part,
        cast(membership_basis as varchar) as membership_basis,
        cast(reason as varchar) as membership_reason,
        cast(reviewed_by as varchar) as reviewed_by,
        try_cast(reviewed_at_utc as timestamp) as reviewed_at_utc
    from {{ source('polymarket_wc2026_raw', 'reviewed_event_membership') }}
),

classified as (
    select
        events.*,
        fixtures.fifa_match_id,
        fixtures.fixture_stage,
        fixtures.group_label as fixture_group_label,
        fixtures.fixture_mapping_basis,
        regexp_matches(
            events.tags_key, '"' || contract.required_event_tag || '"'
        ) as has_required_event_tag,
        regexp_matches(events.tags_key, '"fifa-world-cup"') as has_recall_tag,
        regexp_matches(events.series_key, '"soccer-fifwc"')
            as has_fixture_series,
        case
            when fixtures.event_id is not null then 'sporting'
            when regexp_matches(
                events.title_key || ' ' || events.tags_key, 'qualif'
            ) then 'qualification'
            when regexp_matches(
                events.title_key || ' ' || events.tags_key || ' ' || events.series_key,
                'announcer|mention-markets|world-cup-mentions|wc-culture-mentions|'
                || 'wc-trump|trump|halftime|perform at|shake hands|attend|'
                || 'viewership|cry at'
            ) then 'culture_mentions'
            when regexp_matches(
                events.title_key,
                'relocat|removed from world cup|replace iran|reschedul'
            ) then 'administrative'
            when regexp_matches(
                events.title_key || ' ' || events.tags_key,
                'squad|play in the world cup|wc-player-futures'
            ) then 'pre_tournament_participation'
            when
                regexp_matches(events.tags_key, '"soccer"|"sports"')
                or regexp_matches(events.series_key, '"soccer-fifwc"')
                then 'sporting_candidate'
            else 'other_adjacent'
        end as inferred_membership_class,
        case
            when fixtures.event_id is not null then fixtures.fixture_stage
            when
                regexp_matches(
                    events.title_key || ' ' || events.tags_key,
                    'award|golden boot|golden ball|silver ball|bronze ball|'
                    || 'golden glove|fair play'
                )
                then 'awards'
            when
                regexp_matches(events.title_key, 'winner|champion')
                and not regexp_matches(events.title_key, 'group')
                then 'tournament_wide'
            when regexp_matches(
                events.title_key, 'group [a-l]|group stage|group phase'
            ) then 'group_stage'
            when regexp_matches(
                events.title_key || ' ' || events.slug_key,
                'round of 32|round-of-32'
            ) then 'round_of_32'
            when regexp_matches(
                events.title_key || ' ' || events.slug_key,
                'round of 16|round-of-16'
            ) then 'round_of_16'
            when regexp_matches(
                events.title_key || ' ' || events.slug_key, 'quarter.?final'
            ) then 'quarterfinal'
            when regexp_matches(
                events.title_key || ' ' || events.slug_key, 'semi.?final'
            ) then 'semifinal'
            when regexp_matches(
                events.title_key || ' ' || events.slug_key,
                'third.?place|3rd place'
            ) then 'third_place'
            when regexp_matches(
                events.title_key || ' ' || events.slug_key, 'final'
            ) then 'final'
            when regexp_matches(
                events.title_key, 'squad|play in the world cup|removed|replace'
            ) then 'pre_tournament'
            else 'tournament_wide'
        end as inferred_tournament_part
    from events
    cross join contract
    left join fixtures on events.event_id = fixtures.event_id
),

decisions as (
    select
        classified.*,
        overrides.reviewed_by,
        overrides.reviewed_at_utc,
        overrides.event_id is not null
        and nullif(trim(overrides.membership_basis), '') is not null
        and nullif(trim(overrides.membership_reason), '') is not null
        and nullif(trim(overrides.reviewed_by), '') is not null
        and overrides.reviewed_at_utc is not null
        and (
            overrides.membership_status != 'included'
            or overrides.tournament_part in (
                'tournament_wide', 'group_stage', 'round_of_32',
                'round_of_16', 'quarterfinal', 'semifinal',
                'third_place', 'final', 'awards'
            )
        ) as has_reviewed_decision,
        coalesce(overrides.membership_class, classified.inferred_membership_class)
            as membership_class,
        coalesce(overrides.tournament_part, classified.inferred_tournament_part)
            as tournament_part,
        coalesce(
            case
                when
                    overrides.event_id is not null
                    and nullif(trim(overrides.membership_basis), '') is not null
                    and nullif(trim(overrides.membership_reason), '') is not null
                    and nullif(trim(overrides.reviewed_by), '') is not null
                    and overrides.reviewed_at_utc is not null
                    and (
                        overrides.membership_status != 'included'
                        or overrides.tournament_part in (
                            'tournament_wide', 'group_stage', 'round_of_32',
                            'round_of_16', 'quarterfinal', 'semifinal',
                            'third_place', 'final', 'awards'
                        )
                    ) then overrides.membership_status
            end,
            case
                when classified.fixture_mapping_basis is not null then 'included'
                else 'review_required'
            end
        ) as membership_status,
        coalesce(
            overrides.membership_basis,
            case
                when classified.fixture_mapping_basis is not null
                    then 'exact_official_fixture_mapping'
                else 'unreviewed_candidate'
            end
        ) as membership_basis,
        coalesce(
            overrides.membership_reason,
            case
                when classified.fixture_mapping_basis is not null
                    then 'Uniquely mapped to one official FIFA fixture'
                else 'Non-fixture candidates require an explicit reviewed decision'
            end
        ) as membership_reason
    from classified
    left join overrides on classified.event_id = overrides.event_id
),

projected as (
    select
        -- noqa: disable=RF03
        decisions.* exclude (
            title_key,
            slug_key,
            tags_key,
            series_key,
            inferred_membership_class,
            inferred_tournament_part,
            tournament_part
        ),
        -- noqa: enable=RF03
        case
            when decisions.membership_status = 'included'
                then decisions.tournament_part
        end as tournament_part,
        case
            when decisions.membership_status != 'included' then null
            when decisions.fifa_match_id is not null
                then 'scope:wc2026:fixture:' || decisions.fifa_match_id
            else 'scope:wc2026:' || decisions.tournament_part
        end as scope_id,
        decisions.membership_status = 'included'
        and decisions.ever_eligible as is_logical_event
    from decisions
)

select
    projected.*,
    contract.policy_version as membership_policy_version,
    contract.contract_name,
    contract.contract_version
from projected
cross join contract
