with private_sources (source, required_for_v4) as (
    values
    ('eloratings', true),
    ('fifaindex', true),
    ('wikipedia_squads', true),
    ('clubelo', true),
    ('fotmob', false)
),

private_latest as (
    select
        source,
        max(collected_at) as latest_collected_at,
        arg_max(snapshot_id, collected_at) as latest_snapshot_id
    from {{ source('wc2026_snapshot_ops', 'raw_snapshot_ledger') }}
    group by source
),

private_rows as (
    select
        sources.source,
        sources.required_for_v4,
        latest.latest_snapshot_id is not null as available,
        latest.latest_snapshot_id,
        latest.latest_collected_at,
        case
            when latest.latest_collected_at is null then null
            else date_diff('hour', latest.latest_collected_at, current_timestamp)
        end as age_hours,
        'canonical_snapshot' as availability_mode
    from private_sources as sources
    left join private_latest as latest
        on sources.source = latest.source
),

polymarket_row as (
    select
        'polymarket' as source,
        true as required_for_v4,
        count(*) > 0 as available,
        cast(null as varchar) as latest_snapshot_id,
        max(latest_point_odds_timestamp) as latest_collected_at,
        case
            when max(latest_point_odds_timestamp) is null then null
            else date_diff(
                'hour', max(latest_point_odds_timestamp), current_timestamp
            )
        end as age_hours,
        'public_collector' as availability_mode
    from {{ ref('wc2026_price_liquidity_current') }}
),

static_reference_row as (
    select
        'static_reference' as source,
        true as required_for_v4,
        count(*) = 104 as available,
        'committed' as latest_snapshot_id,
        cast(null as timestamptz) as latest_collected_at,
        cast(0 as bigint) as age_hours,
        'committed_static_seed' as availability_mode
    from {{ ref('wc2026_fixtures') }}
),

international_results_row as (
    select
        'international_results' as source,
        true as required_for_v4,
        count(*) > 0 as available,
        cast(null as varchar) as latest_snapshot_id,
        max(source_loaded_at) as latest_collected_at,
        case
            when max(source_loaded_at) is null then null
            else date_diff('hour', max(source_loaded_at), current_timestamp)
        end as age_hours,
        'public_collector' as availability_mode
    from {{ source(
        'international_results_wc2026_raw', 'historical_matches'
    ) }}
),

public_rows as (
    select * from polymarket_row
    union all
    select * from static_reference_row
    union all
    select * from international_results_row
)

select * from private_rows
union all
select * from public_rows
