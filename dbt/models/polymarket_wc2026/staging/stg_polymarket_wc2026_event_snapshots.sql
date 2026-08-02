{{ config(tags=['wc2026_logical_atlas']) }}

select
    cast(event_id as varchar) as event_id,
    cast(volume_24h_usd as double) as volume_24h_usd,
    cast(volume_1w_usd as double) as volume_1w_usd,
    cast(volume_1m_usd as double) as volume_1m_usd,
    cast(volume_1y_usd as double) as volume_1y_usd,
    cast(liquidity_usd as double) as liquidity_usd,
    cast(open_interest_usd as double) as open_interest_usd,
    cast(is_active as boolean) as is_active,
    cast(is_closed as boolean) as is_closed,
    cast(is_archived as boolean) as is_archived,
    cast(created_at as timestamp) as created_at,
    cast(source_updated_at as timestamp) as source_updated_at,
    cast(start_at as timestamp) as start_at,
    cast(end_at as timestamp) as end_at,
    cast(closed_at as timestamp) as closed_at,
    cast(event_start_at as timestamp) as event_start_at,
    cast(finished_at as timestamp) as finished_at,
    cast(neg_risk as boolean) as neg_risk,
    cast(enable_neg_risk as boolean) as enable_neg_risk,
    cast(show_all_outcomes as boolean) as show_all_outcomes,
    cast(tags_json as varchar) as tags_json,
    cast(series_slugs_json as varchar) as series_slugs_json,
    cast(candidate_sources_json as varchar) as candidate_sources_json,
    cast(source_market_count as bigint) as source_market_count,
    cast(observed_at as timestamp) as observed_at,
    cast(source_endpoint as varchar) as source_endpoint,
    nullif(trim(cast(event_slug as varchar)), '') as event_slug,
    nullif(trim(cast(event_title as varchar)), '') as event_title,
    nullif(trim(cast(event_subtitle as varchar)), '') as event_subtitle,
    nullif(trim(cast(event_description as varchar)), '') as event_description,
    nullif(trim(cast(resolution_source as varchar)), '') as resolution_source,
    case
        when
            cast(event_volume_usd_lifetime_reported as double) >= 0
            and isfinite(cast(event_volume_usd_lifetime_reported as double))
            then cast(event_volume_usd_lifetime_reported as double)
    end as event_volume_usd_lifetime_reported,
    nullif(trim(cast(game_id as varchar)), '') as game_id,
    nullif(trim(cast(parent_event_id as varchar)), '') as parent_event_id,
    nullif(trim(cast(neg_risk_market_id as varchar)), '') as neg_risk_market_id
from {{ source('polymarket_wc2026_raw', 'event_snapshots') }}
