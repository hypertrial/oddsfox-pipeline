{{ config(tags=['wc2026_logical_atlas']) }}

with latest_complete_observations as (
    select
        event_id,
        max(observed_at) as observed_at
    from {{ ref('stg_polymarket_wc2026_event_snapshots') }}
    group by event_id
),

current_links as (
    select
        links.event_id,
        links.market_id,
        min(links.source_ordinal) as source_ordinal,
        bool_or(links.is_enclosing_event) as is_enclosing_event
    from {{ ref('stg_polymarket_wc2026_event_markets') }} as links
    inner join latest_complete_observations as observations
        on
            links.event_id = observations.event_id
            and links.observed_at = observations.observed_at
    group by links.event_id, links.market_id
),

catalog_links as (
    select
        membership.event_id,
        links.market_id,
        links.source_ordinal,
        links.is_enclosing_event,
        membership.created_at as event_created_at,
        membership.is_logical_event as event_logical_eligible,
        membership.membership_status as event_membership_status,
        membership.ever_eligible as event_ever_eligible,
        membership.volume_unknown as event_volume_unknown,
        fixtures.fifa_match_id,
        fixtures.fixture_mapping_basis
    from current_links as links
    inner join {{ ref('int_polymarket_wc2026_event_membership') }} as membership
        on links.event_id = membership.event_id
    left join {{ ref('int_polymarket_wc2026_fixture_events') }} as fixtures
        on links.event_id = fixtures.event_id
),

eligible_markets as (
    select distinct market_id
    from catalog_links
    where event_logical_eligible
),

export_links as (
    select links.*
    from catalog_links as links
    inner join eligible_markets on links.market_id = eligible_markets.market_id
),

eligible_ranked as (
    select
        event_id,
        market_id,
        row_number() over (
            partition by market_id
            order by
                fifa_match_id is not null desc,
                is_enclosing_event desc,
                source_ordinal asc,
                event_created_at asc nulls last,
                event_id asc
        ) as qualifying_event_rank
    from export_links
    where event_logical_eligible
)

select
    links.event_id,
    links.market_id,
    links.source_ordinal,
    links.is_enclosing_event,
    links.event_logical_eligible,
    links.event_membership_status,
    links.event_ever_eligible,
    links.event_volume_unknown,
    links.fifa_match_id,
    links.fixture_mapping_basis,
    coalesce(ranked.qualifying_event_rank = 1, false)
        as is_primary_qualifying_event
from export_links as links
left join eligible_ranked as ranked on links.event_id = ranked.event_id and links.market_id = ranked.market_id
