-- costguard: disable-file=SQLCOST038
-- Hourly odds intentionally joins fact rows to unique market metadata on
-- market_ticker; odds_hour_epoch stays on the fact side and is not a dimension key.
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
    h.odds_hour_utc,
    h.odds_hour_epoch,
    h.open_price as yes_open_price,
    h.high_price as yes_high_price,
    h.low_price as yes_low_price,
    h.close_price as yes_close_price,
    h.avg_price as yes_avg_price,
    h.volume,
    h.latest_refreshed_at,
    case
        when s.market_direction = 'elimination' then 1.0 - h.open_price
        else h.open_price
    end as progression_open_price,
    case
        when s.market_direction = 'elimination' then 1.0 - h.low_price
        else h.high_price
    end as progression_high_price,
    case
        when s.market_direction = 'elimination' then 1.0 - h.high_price
        else h.low_price
    end as progression_low_price,
    case
        when s.market_direction = 'elimination' then 1.0 - h.close_price
        else h.close_price
    end as progression_close_price,
    case
        when s.market_direction = 'elimination' then 1.0 - h.avg_price
        else h.avg_price
    end as progression_avg_price
from {{ ref('int_kalshi_wc2026_market_hourly_odds') }} as h
inner join {{ ref('int_kalshi_wc2026_stage_classification') }} as s
    on h.market_ticker = s.market_ticker
