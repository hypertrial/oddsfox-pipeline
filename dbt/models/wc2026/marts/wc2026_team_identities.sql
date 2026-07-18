{{ config(alias='team_identities') }}

with fixture_names as (
    select home_team as team_name from {{ ref('wc2026_fixtures') }}
    union
    select away_team as team_name from {{ ref('wc2026_fixtures') }}
),

aliases as (
    select
        lower(trim(cast(variant_match_key as varchar))) as variant_match_key,
        lower(trim(cast(canonical_match_key as varchar))) as canonical_match_key
    from {{ ref('wc2026_team_canonical_aliases') }}
),

names as (
    select
        fixture.team_name,
        {{ canonical_team_match_key('fixture.team_name') }} as source_match_key
    from fixture_names as fixture
    where fixture.team_name is not null
)

select
    names.team_name,
    names.source_match_key,
    coalesce(aliases.canonical_match_key, names.source_match_key) as team_match_key,
    md5(coalesce(aliases.canonical_match_key, names.source_match_key))
        as canonical_team_id
from names
left join aliases
    on names.source_match_key = aliases.variant_match_key
