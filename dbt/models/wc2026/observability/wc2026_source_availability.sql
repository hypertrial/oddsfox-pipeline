select * from {{ source('oddsfox_reference', 'wc2026_source_availability') }}

union all

select
    'polymarket' as source,
    true as required_for_v4,
    count(*) > 0 as available,
    cast(null as varchar) as latest_snapshot_id,
    max(latest_point_odds_timestamp) as latest_collected_at,
    case
        when max(latest_point_odds_timestamp) is null then null
        else date_diff('hour', max(latest_point_odds_timestamp), current_timestamp)
    end as age_hours,
    count(*) as row_count,
    'public_collector' as availability_mode
from {{ ref('wc2026_price_liquidity_current') }}
