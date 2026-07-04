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
        'source_state_anomaly:' || market_id || ':' || clob_token_id as issue_key,
        'warn' as severity,
        'token' as entity_type,
        market_id,
        clob_token_id,
        stage_key,
        team_name,
        market_status,
        'Source reports is_active=true and is_closed=true; derived market_status treats it as closed.'
            as issue_detail,
        current_timestamp as observed_at
    from snapshot
    where source_state_anomaly
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
        current_timestamp as observed_at
    from snapshot
    where current_price_status = 'stale_live'
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
        'Raw classified market count above $5k is '
        || cast(coalesce(t.raw_classified_markets_ge_5000, 0) as varchar)
        || ', below the expected '
        || cast(e.minimum_raw_markets_ge_5000 as varchar)
        || ' for source coverage review.' as issue_detail,
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
select * from sparse_stage_coverage
union all
select * from invalid_market_status
union all
select * from invalid_current_price_status
union all
select * from live_contract_conflicts
