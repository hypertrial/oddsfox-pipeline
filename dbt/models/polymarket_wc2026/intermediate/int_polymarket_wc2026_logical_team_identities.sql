{{ config(tags=['wc2026_logical_atlas']) }}

with team_aliases as (
    select
        lower(trim(cast(variant_match_key as varchar))) as variant_match_key,
        lower(trim(cast(canonical_match_key as varchar))) as canonical_match_key
    from {{ ref('wc2026_team_canonical_aliases') }}
),

fixture_names as (
    select home_team as team_name
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }}

    union all

    select away_team as team_name
    from {{ ref('stg_openfootball_wc2026_schedule_fixtures') }}
),

fixture_keys as (
    select distinct
        fixtures.team_name,
        {{ canonical_team_match_key('fixtures.team_name') }} as source_match_key,
        coalesce(
            team_aliases.canonical_match_key,
            {{ canonical_team_match_key('fixtures.team_name') }}
        ) as canonical_match_key
    from fixture_names as fixtures
    left join team_aliases
        on
            {{ canonical_team_match_key('fixtures.team_name') }}
            = team_aliases.variant_match_key
    where nullif(trim(fixtures.team_name), '') is not null
),

canonical_displays as (
    select
        canonical_match_key,
        coalesce(
            min(team_name) filter (
                where source_match_key = canonical_match_key
            ),
            min(team_name),
            canonical_match_key
        ) as team_name
    from fixture_keys
    group by canonical_match_key
),

lookup_candidates as (
    select
        source_match_key,
        canonical_match_key
    from fixture_keys

    union all

    select
        variant_match_key,
        canonical_match_key
    from team_aliases

    union all

    select
        canonical_match_key as source_match_key,
        canonical_match_key
    from team_aliases
),

lookup_keys as (
    select
        source_match_key,
        min(canonical_match_key) as canonical_match_key
    from lookup_candidates
    where
        nullif(source_match_key, '') is not null
        and nullif(canonical_match_key, '') is not null
    group by source_match_key
)

select
    lookup.source_match_key,
    lookup.source_match_key as team_match_key,
    lookup.canonical_match_key,
    coalesce(displays.team_name, lookup.canonical_match_key) as team_name,
    md5(lookup.canonical_match_key) as canonical_team_id
from lookup_keys as lookup
left join canonical_displays as displays
    on lookup.canonical_match_key = displays.canonical_match_key
