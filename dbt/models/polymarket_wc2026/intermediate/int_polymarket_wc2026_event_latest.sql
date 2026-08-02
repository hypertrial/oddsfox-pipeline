{{ config(tags=['wc2026_logical_atlas']) }}

with contract as (
    select cast(event_volume_min_usd as double) as event_volume_min_usd
    from {{ ref('polymarket_wc2026_logical_contract') }}
),

history as (
    select *
    from {{ ref('stg_polymarket_wc2026_event_snapshots') }}
),

eligibility as (
    select
        history.event_id,
        min(history.observed_at) as first_seen_at,
        min(history.observed_at) filter (
            where history.event_volume_usd_lifetime_reported
            >= contract.event_volume_min_usd
        ) as first_eligible_observed_at,
        min(history.created_at) as canonical_created_at,
        count(distinct history.created_at) as event_created_at_variant_count
    from history
    cross join contract
    group by history.event_id
),

latest as (
    select *
    from history
    qualify row_number() over (
        partition by event_id
        order by observed_at desc, source_updated_at desc nulls last
    ) = 1
)

select
    latest.* exclude (created_at),
    eligibility.canonical_created_at as created_at,
    eligibility.first_seen_at,
    eligibility.first_eligible_observed_at,
    eligibility.event_created_at_variant_count,
    contract.event_volume_min_usd,
    eligibility.first_eligible_observed_at is not null as ever_eligible,
    case
        when eligibility.first_eligible_observed_at is not null
            then eligibility.canonical_created_at
    end as eligibility_effective_from,
    latest.event_volume_usd_lifetime_reported is null as volume_unknown,
    coalesce(
        latest.event_volume_usd_lifetime_reported >= contract.event_volume_min_usd,
        false
    ) as currently_eligible
from latest
inner join eligibility on latest.event_id = eligibility.event_id
cross join contract
