with availability as (
    select * from {{ ref('wc2026_source_availability') }}
),

summary as (
    select
        bool_and(available) filter (where required_for_v4)
            as required_sources_available,
        bool_and(
            case
                when source = 'polymarket' then coalesce(age_hours <= 2, false)
                when source in ('eloratings', 'clubelo') then coalesce(age_hours <= 168, false)
                when source in ('fifaindex', 'wikipedia_squads') then coalesce(age_hours <= 720, false)
                when source = 'international_results' then coalesce(age_hours <= 48, false)
                else true
            end
        ) filter (where required_for_v4) as freshness_ok,
        string_agg(
            case
                when required_for_v4 and not available
                    then source || ': unavailable'
                when source = 'polymarket' and coalesce(age_hours > 2, true)
                    then source || ': stale'
                when source in ('eloratings', 'clubelo') and coalesce(age_hours > 168, true)
                    then source || ': stale'
                when
                    source in ('fifaindex', 'wikipedia_squads')
                    and coalesce(age_hours > 720, true)
                    then source || ': stale'
                when source = 'international_results' and coalesce(age_hours > 48, true)
                    then source || ': stale'
            end,
            '; '
        ) filter (
            where
            (required_for_v4 and not available)
            or (source = 'polymarket' and coalesce(age_hours > 2, true))
            or (
                source in ('eloratings', 'clubelo')
                and coalesce(age_hours > 168, true)
            )
            or (
                source in ('fifaindex', 'wikipedia_squads')
                and coalesce(age_hours > 720, true)
            )
            or (
                source = 'international_results'
                and coalesce(age_hours > 48, true)
            )
        ) as blocking_reasons
    from availability
),

point_in_time as (
    select
        count(*) filter (
            where valid_to is not null and valid_to < valid_from
        ) = 0 as point_in_time_ok
    from {{ ref('wc2026_club_strength_history') }}
),

strategies (strategy_id) as (
    values
    ('1_oddsfox_strategy_elo_wc2026_monte_carlo'),
    ('2_oddsfox_strategy_wc2026_monte_carlo_v2'),
    ('4_oddsfox_strategy_wc2026_monte_carlo_v4'),
    ('5_oddsfox_strategy_wc2026_monte_carlo_v5'),
    ('1_oddsfox_strategy_polymarket_wc2026_stage_arbitrage')
)

select
    strategies.strategy_id,
    'wc2026.v1' as required_contract_version,
    summary.blocking_reasons,
    coalesce(summary.required_sources_available, false)
    and coalesce(summary.freshness_ok, false)
    and coalesce(point_in_time.point_in_time_ok, false) as ready,
    coalesce(summary.required_sources_available, false)
        as required_sources_available,
    coalesce(summary.freshness_ok, false) as freshness_ok,
    coalesce(point_in_time.point_in_time_ok, false) as point_in_time_ok,
    current_timestamp as evaluated_at
from strategies
cross join summary
cross join point_in_time
