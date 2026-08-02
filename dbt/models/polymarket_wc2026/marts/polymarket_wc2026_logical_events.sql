{{ config(tags=['wc2026_logical_atlas']) }}

with logical_markets_by_event as (
    select
        links.event_id,
        count(*) as logical_market_count,
        count(*) filter (where markets.logical_usable) as logical_usable_market_count,
        count(distinct markets.market_neg_risk_market_id) filter (
            where markets.market_neg_risk_market_id is not null
        ) as neg_risk_market_set_count,
        min(markets.market_neg_risk_market_id) as neg_risk_market_set_id
    from {{ ref('int_polymarket_wc2026_logical_market_events') }} as links
    inner join {{ ref('int_polymarket_wc2026_logical_markets') }} as markets
        on links.market_id = markets.market_id
    group by links.event_id
)

select
    events.event_id,
    events.event_slug,
    events.event_title,
    events.event_description,
    events.event_description as source_text,
    events.resolution_source,
    events.tags_json,
    events.series_slugs_json,
    events.candidate_sources_json,
    events.event_volume_usd_lifetime_reported,
    events.observed_at as event_volume_observed_at,
    events.first_seen_at,
    events.first_eligible_observed_at,
    events.eligibility_effective_from,
    events.ever_eligible,
    events.currently_eligible,
    events.volume_unknown,
    events.created_at as event_created_at,
    events.is_active,
    events.is_closed,
    events.is_archived,
    events.neg_risk,
    events.neg_risk_market_id,
    events.show_all_outcomes,
    events.end_at,
    events.finished_at,
    events.game_id,
    events.fifa_match_id,
    events.fixture_group_label,
    events.fixture_mapping_basis,
    events.membership_status,
    events.membership_class,
    events.tournament_part,
    events.scope_id,
    events.membership_basis,
    events.membership_reason,
    events.membership_policy_version,
    events.is_logical_event as event_logical_eligible,
    false as event_constraint_complete,
    'https://polymarket.com/event/' || events.event_slug as source_url,
    coalesce(events.event_start_at, events.start_at) as start_at,
    case
        when
            events.is_logical_event
            and coalesce(events.neg_risk, false)
            and events.neg_risk_market_id is not null
            then
                'polymarket:neg-risk-market:'
                || events.neg_risk_market_id || ':positive-outcomes'
        when events.is_logical_event and counts.neg_risk_market_set_count = 1
            then
                'polymarket:neg-risk-market:'
                || counts.neg_risk_market_set_id || ':positive-outcomes'
    end as event_constraint_group_id,
    -- negRisk proves an at-most-one relationship, not that the loaded child
    -- inventory exhausts every possible positive outcome.
    case
        when
            events.is_logical_event and (
                (
                    coalesce(events.neg_risk, false)
                    and events.neg_risk_market_id is not null
                )
                or counts.neg_risk_market_set_count = 1
            )
            then 'at_most_one'
    end as event_constraint_kind
from {{ ref('int_polymarket_wc2026_event_membership') }} as events
left join logical_markets_by_event as counts on events.event_id = counts.event_id
