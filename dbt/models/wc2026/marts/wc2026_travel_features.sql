{{ config(alias='travel_features') }}

with team_base_camps as (
    select
        cast(team_name_model as varchar) as team_name_model,
        {{ canonical_team_match_key('team_name_model') }} as team_match_key,
        cast(base_camp_market as varchar) as base_camp_market,
        cast(base_camp_country as varchar) as base_camp_country,
        cast(training_site_name as varchar) as training_site_name,
        cast(training_site_lat as double) as training_site_lat,
        cast(training_site_lon as double) as training_site_lon,
        cast(training_site_timezone as varchar) as training_site_timezone,
        cast(training_site_altitude_m as double) as training_site_altitude_m,
        cast(geocode_quality as varchar) as geocode_quality
    from {{ ref('wc2026_base_camps_teams') }}
),

team_matches as (
    select
        match_id,
        stage,
        group_label,
        matchday,
        match_date,
        kickoff_time_et,
        kickoff_at_et,
        venue,
        home_team as team_name_model,
        away_team as opponent_team_name_model,
        'home' as team_side
    from {{ ref('wc2026_fixtures') }}
    where stage = 'Group Stage'

    union all

    select
        match_id,
        stage,
        group_label,
        matchday,
        match_date,
        kickoff_time_et,
        kickoff_at_et,
        venue,
        away_team as team_name_model,
        home_team as opponent_team_name_model,
        'away' as team_side
    from {{ ref('wc2026_fixtures') }}
    where stage = 'Group Stage'
),

with_context as (
    select
        matches.*,
        venue.host_city,
        venue.host_country,
        venue.venue_lat,
        venue.venue_lon,
        venue.venue_timezone,
        venue.venue_altitude_m,
        base.base_camp_market,
        base.base_camp_country,
        base.training_site_name,
        base.training_site_lat,
        base.training_site_lon,
        base.training_site_timezone,
        base.training_site_altitude_m,
        base.geocode_quality
    from team_matches as matches
    inner join {{ ref('wc2026_base_camp_venues') }} as venue
        on matches.venue = venue.venue
    inner join team_base_camps as base
        on
            {{ canonical_team_match_key('matches.team_name_model') }}
            = base.team_match_key
),

with_previous as (
    select
        *,
        lag(match_id) over (
            partition by team_name_model order by kickoff_at_et
        ) as previous_match_id,
        lag(kickoff_at_et) over (
            partition by team_name_model order by kickoff_at_et
        ) as previous_match_datetime_utc,
        lag(venue) over (
            partition by team_name_model order by kickoff_at_et
        ) as previous_venue,
        lag(venue_lat) over (
            partition by team_name_model order by kickoff_at_et
        ) as previous_venue_lat,
        lag(venue_lon) over (
            partition by team_name_model order by kickoff_at_et
        ) as previous_venue_lon
    from with_context
),

distances as (
    select
        *,
        {{ haversine_km(
            'training_site_lat', 'training_site_lon', 'venue_lat', 'venue_lon'
        ) }} as base_to_venue_km_gc,
        case
            when previous_venue_lat is null then null
            else {{ haversine_km(
                'previous_venue_lat', 'previous_venue_lon', 'venue_lat', 'venue_lon'
            ) }}
        end as previous_venue_to_current_venue_km_gc,
        case
            when previous_venue_lat is null then null
            else {{ haversine_km(
                'previous_venue_lat', 'previous_venue_lon',
                'training_site_lat', 'training_site_lon'
            ) }}
        end as previous_venue_to_base_km_gc,
        venue_altitude_m - training_site_altitude_m
            as base_to_venue_altitude_delta_m,
        case when training_site_timezone = venue_timezone then 0 else 1 end
            as base_to_venue_timezone_changed,
        case
            when previous_match_datetime_utc is null then null
            else date_diff('day', previous_match_datetime_utc, kickoff_at_et)
        end as days_since_previous_match
    from with_previous
),

features as (
    select
        *,
        coalesce(
            least(
                previous_venue_to_current_venue_km_gc,
                previous_venue_to_base_km_gc + base_to_venue_km_gc
            ),
            base_to_venue_km_gc
        ) as effective_travel_km_gc,
        greatest(0.0, 4.0 - coalesce(days_since_previous_match, 4.0))
            as negative_rest_pressure,
        ln(1.0 + coalesce(
            least(
                previous_venue_to_current_venue_km_gc,
                previous_venue_to_base_km_gc + base_to_venue_km_gc
            ),
            base_to_venue_km_gc
        )) as log1p_effective_travel_km,
        abs(venue_altitude_m - training_site_altitude_m) as altitude_delta_abs_m
    from distances
),

scored as (
    select
        *,
        {{ safe_zscore('log1p_effective_travel_km') }}
            as z_log1p_effective_travel_km,
        {{ safe_zscore('base_to_venue_timezone_changed') }} as z_timezone_change,
        {{ safe_zscore('negative_rest_pressure') }} as z_negative_rest_pressure,
        {{ safe_zscore('altitude_delta_abs_m') }} as z_altitude_delta_abs_m
    from features
)

select
    *,
    (
        0.40 * z_log1p_effective_travel_km
        + 0.25 * z_timezone_change
        + 0.20 * z_negative_rest_pressure
        + 0.15 * z_altitude_delta_abs_m
    ) as travel_burden_score
from scored
