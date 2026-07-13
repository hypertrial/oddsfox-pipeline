-- costguard: disable-file=SQLCOST012
-- costguard: disable-file=SQLCOST038
with polymarket_mapping as (
    select
        fifa_match_id,
        max(market_id) as polymarket_market_id,
        max(case when team_side = 'home' then clob_token_id end)
            as polymarket_home_clob_token_id,
        max(case when team_side = 'away' then clob_token_id end)
            as polymarket_away_clob_token_id
    from {{ ref('int_polymarket_wc2026_match_advance_tokens') }}
    where not is_ambiguous_mapping
    group by fifa_match_id
),

polymarket_hourly as (
    select
        m.fifa_match_id,
        h.odds_hour_utc,
        h.odds_hour_epoch,
        max(case when m.team_side = 'home' then h.close_price end)
            as polymarket_home_advance_price,
        max(case when m.team_side = 'away' then h.close_price end)
            as polymarket_away_advance_price,
        max(case when m.team_side = 'home' then h.observed_points end)
            as polymarket_home_observation_count,
        max(case when m.team_side = 'away' then h.observed_points end)
            as polymarket_away_observation_count
    from {{ ref('int_polymarket_wc2026_match_hourly_odds') }} as h
    inner join {{ ref('int_polymarket_wc2026_match_advance_tokens') }} as m
        on h.clob_token_id = m.clob_token_id
    where not m.is_ambiguous_mapping
    group by m.fifa_match_id, h.odds_hour_utc, h.odds_hour_epoch
),

kalshi_mapping as (
    select
        fifa_match_id,
        max(event_ticker) as kalshi_event_ticker,
        max(case when team_side = 'home' then market_ticker end)
            as kalshi_home_market_ticker,
        max(case when team_side = 'away' then market_ticker end)
            as kalshi_away_market_ticker
    from {{ ref('int_kalshi_wc2026_match_advance_markets') }}
    where not is_ambiguous_mapping
    group by fifa_match_id
),

kalshi_hourly as (
    select
        m.fifa_match_id,
        h.odds_hour_utc,
        h.odds_hour_epoch,
        max(case when m.team_side = 'home' then h.close_price end)
            as kalshi_home_advance_price,
        max(case when m.team_side = 'away' then h.close_price end)
            as kalshi_away_advance_price,
        max(case when m.team_side = 'home' then h.volume end)
            as kalshi_home_hourly_volume,
        max(case when m.team_side = 'away' then h.volume end)
            as kalshi_away_hourly_volume
    from {{ ref('int_kalshi_wc2026_match_hourly_odds') }} as h
    inner join {{ ref('int_kalshi_wc2026_match_advance_markets') }} as m
        on h.market_ticker = m.market_ticker
    where not m.is_ambiguous_mapping
    group by m.fifa_match_id, h.odds_hour_utc, h.odds_hour_epoch
),

observed_hours as (
    select
        fifa_match_id,
        odds_hour_utc
    from polymarket_hourly
    union all
    select
        fifa_match_id,
        odds_hour_utc
    from kalshi_hourly
),

bounds as (
    select
        fifa_match_id,
        min(odds_hour_utc) as first_hour_utc,
        max(odds_hour_utc) as last_hour_utc
    from observed_hours
    group by fifa_match_id
),

spine as (
    select
        b.fifa_match_id,
        hourly_spine.odds_hour_utc,
        cast(epoch(hourly_spine.odds_hour_utc) as bigint) as odds_hour_epoch
    from bounds as b
    -- costguard: allow cross-join, at most one short tournament range per match.
    cross join unnest(
        generate_series(b.first_hour_utc, b.last_hour_utc, interval '1 hour')
    ) as hourly_spine (odds_hour_utc)
)

select
    s.odds_hour_utc,
    s.odds_hour_epoch,
    f.fifa_match_id,
    f.stage_key,
    f.stage_rank,
    f.kickoff_at_utc,
    f.home_team,
    f.away_team,
    'team_advances' as price_represents,
    'hourly_close' as price_statistic,
    ph.polymarket_home_advance_price,
    ph.polymarket_away_advance_price,
    kh.kalshi_home_advance_price,
    kh.kalshi_away_advance_price,
    pm.polymarket_market_id,
    pm.polymarket_home_clob_token_id,
    pm.polymarket_away_clob_token_id,
    km.kalshi_event_ticker,
    km.kalshi_home_market_ticker,
    km.kalshi_away_market_ticker,
    ph.polymarket_home_observation_count,
    ph.polymarket_away_observation_count,
    kh.kalshi_home_hourly_volume,
    kh.kalshi_away_hourly_volume,
    case
        when
            ph.polymarket_home_advance_price is not null
            and ph.polymarket_away_advance_price is not null
            then ph.polymarket_home_advance_price + ph.polymarket_away_advance_price
    end as polymarket_pair_price_sum,
    case
        when
            kh.kalshi_home_advance_price is not null
            and kh.kalshi_away_advance_price is not null
            then kh.kalshi_home_advance_price + kh.kalshi_away_advance_price
    end as kalshi_pair_price_sum,
    ph.polymarket_home_advance_price is not null
    and ph.polymarket_away_advance_price is not null as polymarket_hour_complete,
    kh.kalshi_home_advance_price is not null
    and kh.kalshi_away_advance_price is not null as kalshi_hour_complete,
    ph.polymarket_home_advance_price is not null
    and ph.polymarket_away_advance_price is not null
    and kh.kalshi_home_advance_price is not null
    and kh.kalshi_away_advance_price is not null as both_sources_complete,
    s.odds_hour_utc < f.kickoff_at_utc as is_pre_kickoff
from spine as s
inner join {{ ref('int_wc2026_knockout_fixtures') }} as f
    on s.fifa_match_id = f.fifa_match_id
left join polymarket_mapping as pm
    on s.fifa_match_id = pm.fifa_match_id
left join polymarket_hourly as ph
    on
        s.fifa_match_id = ph.fifa_match_id
        and s.odds_hour_epoch = ph.odds_hour_epoch
left join kalshi_mapping as km
    on s.fifa_match_id = km.fifa_match_id
left join kalshi_hourly as kh
    on
        s.fifa_match_id = kh.fifa_match_id
        and s.odds_hour_epoch = kh.odds_hour_epoch
