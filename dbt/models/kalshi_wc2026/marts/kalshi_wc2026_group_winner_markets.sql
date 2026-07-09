with contract as (
    select live_freshness_hours
    from {{ ref('kalshi_wc2026_contract') }}
    where scope_name = 'wc2026'
),

current_market_prices as (
    select
        market_ticker,
        arg_max(close_price, odds_hour_epoch) as group_winner_price,
        arg_max(odds_hour_utc, odds_hour_epoch) as current_price_hour_utc,
        max(odds_hour_epoch) as current_price_hour_epoch
    from {{ ref('kalshi_wc2026_group_winner_market_hourly_odds') }}
    group by 1
),

with_prices as (
    select
        g.market_ticker,
        g.event_ticker,
        g.series_ticker,
        g.event_suffix,
        g.market_suffix,
        g.title,
        g.subtitle,
        g.yes_sub_title,
        g.no_sub_title,
        g.status,
        g.market_type,
        g.open_time,
        g.close_time,
        g.expiration_time,
        g.market_volume,
        g.open_interest,
        g.last_price,
        g.scraped_at,
        g.scope_name,
        g.team_name,
        g.group_letter,
        g.price_represents,
        g.canonical_team_name,
        g.tournament_status,
        g.is_still_alive,
        g.eliminated_stage_key,
        g.eliminated_match_date,
        g.next_match_date,
        g.next_stage_key,
        g.matches_played,
        g.wins,
        g.draws,
        g.losses,
        g.goals_for,
        g.goals_against,
        g.latest_completed_match_date,
        g.latest_completed_stage_key,
        g.market_status,
        g.is_live_market,
        g.source_state_anomaly,
        p.group_winner_price,
        p.current_price_hour_utc,
        p.current_price_hour_epoch,
        contract.live_freshness_hours,
        case
            when p.current_price_hour_epoch is not null
                then round((epoch(current_timestamp) - p.current_price_hour_epoch) / 3600.0, 4)
        end as current_price_age_hours
    from {{ ref('int_kalshi_wc2026_group_winner_classification') }} as g
    left join current_market_prices as p
        on g.market_ticker = p.market_ticker
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
    group_letter,
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
    group_winner_price,
    current_price_hour_utc,
    current_price_hour_epoch,
    current_price_age_hours,
    case
        when market_status = 'resolved' then 'historical_resolved'
        when market_status = 'closed' then 'historical_closed'
        when market_status = 'inactive' then 'inactive'
        when market_status = 'live' and group_winner_price is null then 'missing_live'
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
