-- costguard: disable-file=SQLCOST038
-- Every joined CTE below is explicitly grouped to one row per join key; Costguard
-- cannot infer that reduced grain through CTE lineage from the hourly unique keys.
with expected_fixtures as (
    select fifa_match_id
    from unnest(list_concat(range(73, 103), [104])) as expected_ids (fifa_match_id)
),

polymarket_mapping as (
    select
        fifa_match_id,
        max(markets_per_fixture) as market_count,
        max(case when team_side = 'home' then 1 else 0 end)
        + max(case when team_side = 'away' then 1 else 0 end) as side_count,
        bool_or(is_ambiguous_mapping) as is_ambiguous,
        bool_or(is_active and not is_closed and not is_resolved) as is_active
    from {{ ref('int_polymarket_wc2026_match_advance_tokens') }}
    group by fifa_match_id
),

polymarket_token_hours as (
    select
        clob_token_id,
        min(odds_hour_utc) as first_hour_utc,
        max(odds_hour_utc) as last_hour_utc
    from {{ ref('int_polymarket_wc2026_match_hourly_odds') }}
    group by clob_token_id
),

polymarket_hours as (
    select
        m.fifa_match_id,
        min(h.first_hour_utc) as first_hour_utc,
        max(h.last_hour_utc) as last_hour_utc
    from {{ ref('int_polymarket_wc2026_match_advance_tokens') }} as m
    inner join polymarket_token_hours as h on m.clob_token_id = h.clob_token_id
    where not m.is_ambiguous_mapping
    group by m.fifa_match_id
),

kalshi_mapping as (
    select
        fifa_match_id,
        max(events_per_fixture) as event_count,
        max(case when team_side = 'home' then 1 else 0 end)
        + max(case when team_side = 'away' then 1 else 0 end) as side_count,
        bool_or(is_ambiguous_mapping) as is_ambiguous,
        bool_or(lower(coalesce(status, '')) = 'active') as is_active
    from {{ ref('int_kalshi_wc2026_match_advance_markets') }}
    group by fifa_match_id
),

kalshi_market_hours as (
    select
        market_ticker,
        min(odds_hour_utc) as first_hour_utc,
        max(odds_hour_utc) as last_hour_utc
    from {{ ref('int_kalshi_wc2026_match_hourly_odds') }}
    group by market_ticker
),

kalshi_hours as (
    select
        m.fifa_match_id,
        min(h.first_hour_utc) as first_hour_utc,
        max(h.last_hour_utc) as last_hour_utc
    from {{ ref('int_kalshi_wc2026_match_advance_markets') }} as m
    inner join kalshi_market_hours as h on m.market_ticker = h.market_ticker
    where not m.is_ambiguous_mapping
    group by m.fifa_match_id
),

coverage as (
    select
        e.fifa_match_id,
        f.stage_key,
        f.kickoff_at_utc,
        f.home_team,
        f.away_team,
        ph.first_hour_utc as polymarket_first_hour_utc,
        ph.last_hour_utc as polymarket_last_hour_utc,
        kh.first_hour_utc as kalshi_first_hour_utc,
        kh.last_hour_utc as kalshi_last_hour_utc,
        coalesce(f.teams_resolved, false) as fixture_ready,
        coalesce(pm.market_count, 0) as polymarket_market_count,
        coalesce(pm.side_count, 0) as polymarket_side_count,
        coalesce(pm.is_ambiguous, false) as polymarket_mapping_ambiguous,
        coalesce(pm.market_count = 1 and pm.side_count = 2 and not pm.is_ambiguous, false)
            as polymarket_mapping_ready,
        coalesce(km.event_count, 0) as kalshi_event_count,
        coalesce(km.side_count, 0) as kalshi_side_count,
        coalesce(km.is_ambiguous, false) as kalshi_mapping_ambiguous,
        coalesce(km.event_count = 1 and km.side_count = 2 and not km.is_ambiguous, false)
            as kalshi_mapping_ready,
        coalesce(pm.is_active, false) as polymarket_market_active,
        coalesce(km.is_active, false) as kalshi_market_active
    from expected_fixtures as e
    left join {{ ref('int_wc2026_knockout_fixtures') }} as f
        on e.fifa_match_id = f.fifa_match_id
    left join polymarket_mapping as pm on e.fifa_match_id = pm.fifa_match_id
    left join polymarket_hours as ph on e.fifa_match_id = ph.fifa_match_id
    left join kalshi_mapping as km on e.fifa_match_id = km.fifa_match_id
    left join kalshi_hours as kh on e.fifa_match_id = kh.fifa_match_id
)

select
    *,
    polymarket_mapping_ready and kalshi_mapping_ready as both_platforms_mapped,
    fixture_ready
    and kickoff_at_utc between current_timestamp and current_timestamp + interval '24 hour'
    and not (polymarket_mapping_ready and kalshi_mapping_ready)
        as warning_missing_vendor_mapping,
    polymarket_market_active
    and (
        polymarket_last_hour_utc is null
        or polymarket_last_hour_utc < current_timestamp - interval '3 hour'
    ) as warning_polymarket_stale,
    kalshi_market_active
    and (
        kalshi_last_hour_utc is null
        or kalshi_last_hour_utc < current_timestamp - interval '3 hour'
    ) as warning_kalshi_stale,
    case
        when not fixture_ready then 'fixture_pending'
        when polymarket_mapping_ambiguous or kalshi_mapping_ambiguous then 'mapping_ambiguous'
        when not polymarket_mapping_ready and not kalshi_mapping_ready then 'missing_both'
        when not polymarket_mapping_ready then 'missing_polymarket'
        when not kalshi_mapping_ready then 'missing_kalshi'
        when
            polymarket_market_active
            and (
                polymarket_last_hour_utc is null
                or polymarket_last_hour_utc < current_timestamp - interval '3 hour'
            )
            or kalshi_market_active
            and (
                kalshi_last_hour_utc is null
                or kalshi_last_hour_utc < current_timestamp - interval '3 hour'
            )
            then 'stale'
        else 'ready'
    end as coverage_status
from coverage
