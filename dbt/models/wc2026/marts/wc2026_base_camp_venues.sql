{{ config(alias='base_camp_venues') }}

select
    cast(nullif(trim(venue), '') as varchar) as venue,
    cast(nullif(trim(host_city), '') as varchar) as host_city,
    cast(nullif(trim(host_country), '') as varchar) as host_country,
    cast(venue_lat as double) as venue_lat,
    cast(venue_lon as double) as venue_lon,
    cast(nullif(trim(venue_timezone), '') as varchar) as venue_timezone,
    cast(venue_altitude_m as double) as venue_altitude_m,
    'committed_static_seed' as source_provenance
from {{ ref('wc2026_venues') }}
