with snapshot as (
    select *
    from {{ ref('polymarket_wc2026_knockout_markets') }}
),

stage_totals as (
    select
        stage_key,
        sum(raw_classified_markets_ge_5000) as raw_classified_markets_ge_5000,
        sum(scoped_markets) as scoped_markets
    from {{ ref('polymarket_wc2026_knockout_stage_coverage') }}
    group by 1
),

stage_expectations as (
    select
        expectations.stage_key,
        expectations.minimum_raw_markets_ge_5000
    from (
        values
        ('round_of_32', 32)
    ) as expectations (stage_key, minimum_raw_markets_ge_5000)
),

source_state_anomalies as (
    select
        'source_state_anomaly:'
        || stage_key
        || ':'
        || market_direction
        || ':'
        || market_status as issue_key,
        'warn' as severity,
        'stage' as entity_type,
        cast(null as varchar) as market_id,
        cast(null as varchar) as clob_token_id,
        stage_key,
        cast(null as varchar) as team_name,
        market_status,
        'Polymarket source reports is_active=true and is_closed=true for '
        || cast(count(*) as varchar)
        || ' scoped tokens in this stage/direction/status bucket; derived market_status treats them as closed.'
            as issue_detail,
        count(*) as issue_count,
        current_timestamp as observed_at
    from snapshot
    where source_state_anomaly
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

missing_hourly_odds as (
    select
        case
            when market_status = 'live' then 'live_missing_hourly_odds:'
            when market_status in ('closed', 'resolved') then 'historical_missing_hourly_odds:'
            else 'inactive_missing_hourly_odds:'
        end || market_id || ':' || clob_token_id as issue_key,
        'warn' as severity,
        'token' as entity_type,
        market_id,
        clob_token_id,
        stage_key,
        team_name,
        market_status,
        'Scoped knockout token has no trailing 30-day hourly odds rows.' as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot
    where current_price is null
),

stale_live_odds as (
    select
        'live_stale_hourly_odds:' || market_id || ':' || clob_token_id as issue_key,
        'warn' as severity,
        'token' as entity_type,
        market_id,
        clob_token_id,
        stage_key,
        team_name,
        market_status,
        'Live knockout token latest hourly odds are older than 3 hours.' as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot
    where current_price_status = 'stale_live'
),

active_result_teams as (
    select team_name
    from {{ ref('international_results_wc2026_team_status') }}
    where tournament_status = 'active'
),

live_odds_teams as (
    select
        canonical_team_name as team_name,
        count(*) as live_market_rows
    from snapshot
    where is_live_market
    group by 1
),

active_team_missing_live_odds as (
    select
        'active_team_missing_live_knockout_odds:' || r.team_name as issue_key,
        'warn' as severity,
        'team' as entity_type,
        cast(null as varchar) as market_id,
        cast(null as varchar) as clob_token_id,
        cast(null as varchar) as stage_key,
        r.team_name,
        cast(null as varchar) as market_status,
        'Active WC2026 team has no live Polymarket knockout odds row in the public $5k scope.'
            as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from active_result_teams as r
    left join live_odds_teams as l
        on lower(r.team_name) = lower(l.team_name)
    where l.team_name is null
),

live_odds_non_active_team as (
    select
        'live_knockout_odds_non_active_team:' || l.team_name as issue_key,
        'warn' as severity,
        'team' as entity_type,
        cast(null as varchar) as market_id,
        cast(null as varchar) as clob_token_id,
        cast(null as varchar) as stage_key,
        l.team_name,
        'live' as market_status,
        'Live Polymarket knockout odds exist for a team not active in the WC2026 result mart.'
            as issue_detail,
        l.live_market_rows as issue_count,
        current_timestamp as observed_at
    from live_odds_teams as l
    left join active_result_teams as r
        on lower(l.team_name) = lower(r.team_name)
    where r.team_name is null
),

sparse_stage_coverage as (
    select
        'sparse_stage_coverage:' || e.stage_key as issue_key,
        'warn' as severity,
        'stage' as entity_type,
        cast(null as varchar) as market_id,
        cast(null as varchar) as clob_token_id,
        e.stage_key,
        cast(null as varchar) as team_name,
        cast(null as varchar) as market_status,
        'Upstream Polymarket raw classified market coverage above $5k is '
        || cast(coalesce(t.raw_classified_markets_ge_5000, 0) as varchar)
        || ', below the expected '
        || cast(e.minimum_raw_markets_ge_5000 as varchar)
        || '; this is source availability, not public mart filtering.' as issue_detail,
        e.minimum_raw_markets_ge_5000 - coalesce(t.raw_classified_markets_ge_5000, 0)
            as issue_count,
        current_timestamp as observed_at
    from stage_expectations as e
    left join stage_totals as t
        on e.stage_key = t.stage_key
    where coalesce(t.raw_classified_markets_ge_5000, 0) < e.minimum_raw_markets_ge_5000
),

invalid_market_status as (
    select
        'invalid_market_status:' || market_id || ':' || clob_token_id as issue_key,
        'error' as severity,
        'token' as entity_type,
        market_id,
        clob_token_id,
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
        'invalid_current_price_status:' || market_id || ':' || clob_token_id as issue_key,
        'error' as severity,
        'token' as entity_type,
        market_id,
        clob_token_id,
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
        'live_contract_conflict:' || market_id || ':' || clob_token_id as issue_key,
        'error' as severity,
        'token' as entity_type,
        market_id,
        clob_token_id,
        stage_key,
        team_name,
        market_status,
        'Derived live market conflicts with source active/closed/resolved flags.' as issue_detail,
        1 as issue_count,
        current_timestamp as observed_at
    from snapshot
    where
        market_status = 'live'
        and (
            not coalesce(is_active, false)
            or coalesce(is_closed, false)
            or coalesce(is_resolved, false)
        )
)

select * from source_state_anomalies
union all
select * from missing_hourly_odds
union all
select * from stale_live_odds
union all
select * from active_team_missing_live_odds
union all
select * from live_odds_non_active_team
union all
select * from sparse_stage_coverage
union all
select * from invalid_market_status
union all
select * from invalid_current_price_status
union all
select * from live_contract_conflicts
