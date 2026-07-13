with polymarket_ambiguous as (
    select
        'error' as severity,
        'ambiguous_polymarket_mapping' as issue_type,
        fifa_match_id,
        market_id as source_identifier,
        'More than one Polymarket advance market mapped to this FIFA fixture' as details
    from {{ ref('int_polymarket_wc2026_match_advance_tokens') }}
    where is_ambiguous_mapping
    group by fifa_match_id, market_id
),

kalshi_ambiguous as (
    select
        'error' as severity,
        'ambiguous_kalshi_mapping' as issue_type,
        fifa_match_id,
        event_ticker as source_identifier,
        'More than one Kalshi advance event mapped to this FIFA fixture' as details
    from {{ ref('int_kalshi_wc2026_match_advance_markets') }}
    where is_ambiguous_mapping
    group by fifa_match_id, event_ticker
),

invalid_prices as (
    select
        'error' as severity,
        'price_out_of_range' as issue_type,
        fifa_match_id,
        cast(odds_hour_epoch as varchar) as source_identifier,
        'At least one platform price is outside [0, 1]' as details
    from {{ ref('wc2026_knockout_match_hourly_odds') }}
    where
        (polymarket_home_advance_price not between 0 and 1 and polymarket_home_advance_price is not null)
        or (polymarket_away_advance_price not between 0 and 1 and polymarket_away_advance_price is not null)
        or (kalshi_home_advance_price not between 0 and 1 and kalshi_home_advance_price is not null)
        or (kalshi_away_advance_price not between 0 and 1 and kalshi_away_advance_price is not null)
),

invalid_fixtures as (
    select
        'error' as severity,
        case
            when fifa_match_id = 103 then 'third_place_included'
            when home_team = away_team then 'identical_fixture_teams'
            else 'invalid_stage_match_id'
        end as issue_type,
        fifa_match_id,
        cast(fifa_match_id as varchar) as source_identifier,
        'Fixture identity violates the FIFA knockout contract' as details
    from {{ ref('int_wc2026_knockout_fixtures') }}
    where
        fifa_match_id = 103
        or home_team = away_team
        or not (
            (fifa_match_id between 73 and 88 and stage_key = 'round_of_32')
            or (fifa_match_id between 89 and 96 and stage_key = 'round_of_16')
            or (fifa_match_id between 97 and 100 and stage_key = 'quarterfinal')
            or (fifa_match_id between 101 and 102 and stage_key = 'semifinal')
            or (fifa_match_id = 104 and stage_key = 'final')
        )
),

coverage_warnings as (
    select
        'warning' as severity,
        'missing_vendor_mapping_near_kickoff' as issue_type,
        fifa_match_id,
        cast(null as varchar) as source_identifier,
        coverage_status as details
    from {{ ref('wc2026_knockout_match_odds_coverage') }}
    where warning_missing_vendor_mapping

    union all

    select
        'warning' as severity,
        'stale_polymarket_market' as issue_type,
        fifa_match_id,
        cast(null as varchar) as source_identifier,
        coverage_status as details
    from {{ ref('wc2026_knockout_match_odds_coverage') }}
    where warning_polymarket_stale

    union all

    select
        'warning' as severity,
        'stale_kalshi_market' as issue_type,
        fifa_match_id,
        cast(null as varchar) as source_identifier,
        coverage_status as details
    from {{ ref('wc2026_knockout_match_odds_coverage') }}
    where warning_kalshi_stale
)

select * from polymarket_ambiguous
union all
select * from kalshi_ambiguous
union all
select * from invalid_prices
union all
select * from invalid_fixtures
union all
select * from coverage_warnings
