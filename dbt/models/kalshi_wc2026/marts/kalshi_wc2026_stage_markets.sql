with contract as (
    select live_freshness_hours
    from {{ ref('kalshi_wc2026_contract') }}
    where scope_name = 'wc2026'
),

current_market_prices as (
    select
        market_ticker,
        arg_max(progression_close_price, odds_hour_epoch) as current_price,
        arg_max(odds_hour_utc, odds_hour_epoch) as current_price_hour_utc,
        max(odds_hour_epoch) as current_price_hour_epoch
    from {{ ref('kalshi_wc2026_stage_market_hourly_odds') }}
    group by 1
),

with_prices as (
    select
        s.market_ticker,
        s.event_ticker,
        s.series_ticker,
        s.event_suffix,
        s.market_suffix,
        s.title,
        s.subtitle,
        s.yes_sub_title,
        s.no_sub_title,
        s.status,
        s.market_type,
        s.open_time,
        s.close_time,
        s.expiration_time,
        s.market_volume,
        s.open_interest,
        s.last_price,
        s.scraped_at,
        s.scope_name,
        s.team_name,
        s.stage_key,
        s.stage_rank,
        s.market_direction,
        s.progression_outcome_label,
        s.price_represents,
        s.canonical_team_name,
        s.tournament_status,
        s.is_still_alive,
        s.eliminated_stage_key,
        s.eliminated_match_date,
        s.next_match_date,
        s.next_stage_key,
        s.matches_played,
        s.wins,
        s.draws,
        s.losses,
        s.goals_for,
        s.goals_against,
        s.latest_completed_match_date,
        s.latest_completed_stage_key,
        s.market_status,
        s.is_live_market,
        s.source_state_anomaly,
        p.current_price,
        p.current_price_hour_utc,
        p.current_price_hour_epoch,
        contract.live_freshness_hours,
        case
            when p.current_price_hour_epoch is not null
                then round((epoch(current_timestamp) - p.current_price_hour_epoch) / 3600.0, 4)
        end as current_price_age_hours
    from {{ ref('int_kalshi_wc2026_stage_classification') }} as s
    left join current_market_prices as p
        on s.market_ticker = p.market_ticker
    -- costguard: allow cross-join, WC2026 contract seed has one row.
    cross join contract
)

select
    market_ticker,
    event_ticker,
    series_ticker,
    event_suffix,
    market_suffix,
    title,
    subtitle,
    yes_sub_title,
    no_sub_title,
    status,
    market_type,
    open_time,
    close_time,
    expiration_time,
    market_volume,
    open_interest,
    last_price,
    scraped_at,
    scope_name,
    team_name,
    stage_key,
    stage_rank,
    market_direction,
    progression_outcome_label,
    price_represents,
    canonical_team_name,
    tournament_status,
    is_still_alive,
    eliminated_stage_key,
    eliminated_match_date,
    next_match_date,
    next_stage_key,
    matches_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    latest_completed_match_date,
    latest_completed_stage_key,
    market_status,
    is_live_market,
    source_state_anomaly,
    current_price as progression_price,
    current_price_hour_utc,
    current_price_hour_epoch,
    current_price_age_hours,
    case
        when market_status = 'resolved' then 'historical_resolved'
        when market_status = 'closed' then 'historical_closed'
        when market_status = 'inactive' then 'inactive'
        when market_status = 'live' and current_price is null then 'missing_live'
        when market_status = 'live' and current_price_age_hours <= live_freshness_hours then 'fresh_live'
        when market_status = 'live' then 'stale_live'
    end as current_price_status,
    coalesce(
        market_status = 'live' and current_price_age_hours <= live_freshness_hours,
        false
    ) as is_current_price_fresh,
    coalesce(
        is_still_alive
        and is_live_market
        and market_status = 'live'
        and current_price_age_hours <= live_freshness_hours,
        false
    ) as is_actionable_live_market
from with_prices
