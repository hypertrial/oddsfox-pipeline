with contract as (
    select *
    from {{ ref('kalshi_wc2026_contract') }}
    where scope_name = 'wc2026'
),

snapshot as (
    select *
    from {{ ref('kalshi_wc2026_stage_markets') }}
),

active_result_teams as (
    select team_name
    from {{ ref('international_results_wc2026_team_status') }}
    where tournament_status = 'active'
),

eliminated_result_teams as (
    select
        team_name,
        eliminated_stage_key,
        eliminated_match_date
    from {{ ref('international_results_wc2026_team_status') }}
    where tournament_status != 'active'
),

live_odds_teams as (
    select
        canonical_team_name as team_name,
        count(*) as live_market_rows
    from snapshot
    where is_live_market
    group by 1
),

missing_hourly_odds as (
    select
        case
            when market_status = 'live' then 'live_missing_hourly_odds:'
            when market_status in ('closed', 'resolved') then 'historical_missing_hourly_odds:'
            else 'inactive_missing_hourly_odds:'
        end || market_ticker as issue_key,
        'warn' as severity,
        'market' as entity_type,
        market_ticker,
        stage_key,
        team_name,
        market_status,
        'Scoped Kalshi stage market has no trailing tournament-window hourly odds rows.'
            as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot
    where
        progression_price is null
        and (market_status != 'live' or is_still_alive)
),

stale_live_odds as (
    select
        'live_stale_hourly_odds:' || s.market_ticker as issue_key,
        'warn' as severity,
        'market' as entity_type,
        s.market_ticker,
        s.stage_key,
        s.team_name,
        s.market_status,
        'Live Kalshi stage market latest hourly odds are older than '
        || cast(contract.live_freshness_hours as varchar)
        || ' hours.' as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot as s
    -- costguard: allow cross-join, WC2026 contract seed has one row.
    cross join contract
    where
        s.current_price_status = 'stale_live'
        and s.is_still_alive
),

active_team_missing_live_odds as (
    select
        'active_team_missing_live_stage_odds:' || r.team_name as issue_key,
        'warn' as severity,
        'team' as entity_type,
        cast(null as varchar) as market_ticker,
        cast(null as varchar) as stage_key,
        r.team_name,
        cast(null as varchar) as market_status,
        'Active WC2026 team has no live Kalshi stage odds row in the public scope.'
            as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from active_result_teams as r
    left join live_odds_teams as l
        on lower(r.team_name) = lower(l.team_name)
    where l.team_name is null
),

live_odds_for_eliminated_team as (
    select
        'live_stage_odds_for_eliminated_team:' || l.team_name as issue_key,
        'warn' as severity,
        'team' as entity_type,
        cast(null as varchar) as market_ticker,
        r.eliminated_stage_key as stage_key,
        l.team_name,
        'live' as market_status,
        'Kalshi still marks live stage odds for a team eliminated in the WC2026 result mart; '
        || 'treat this as upstream source lag, not actionable live odds.'
            as issue_detail,
        l.live_market_rows as issue_count,
        current_timestamp as observed_at
    from live_odds_teams as l
    inner join eliminated_result_teams as r
        on lower(l.team_name) = lower(r.team_name)
),

invalid_market_status as (
    select
        'invalid_market_status:' || market_ticker as issue_key,
        'error' as severity,
        'market' as entity_type,
        market_ticker,
        stage_key,
        team_name,
        market_status,
        'Derived market_status is outside the public contract.' as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot
    where market_status is null or market_status not in ('resolved', 'closed', 'live', 'inactive')
),

invalid_current_price_status as (
    select
        'invalid_current_price_status:' || market_ticker as issue_key,
        'error' as severity,
        'market' as entity_type,
        market_ticker,
        stage_key,
        team_name,
        market_status,
        'Derived current_price_status is outside the public contract.' as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot
    where
        current_price_status is null
        or current_price_status not in (
            'fresh_live',
            'stale_live',
            'missing_live',
            'historical_closed',
            'historical_resolved',
            'inactive'
        )
),

live_contract_conflicts as (
    select
        'live_contract_conflict:' || market_ticker as issue_key,
        'error' as severity,
        'market' as entity_type,
        market_ticker,
        stage_key,
        team_name,
        market_status,
        'Derived live market conflicts with source status flags.' as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot
    where
        market_status = 'live'
        and lower(status) != 'active'
)

select * from missing_hourly_odds
union all
select * from stale_live_odds
union all
select * from active_team_missing_live_odds
union all
select * from live_odds_for_eliminated_team
union all
select * from invalid_market_status
union all
select * from invalid_current_price_status
union all
select * from live_contract_conflicts
