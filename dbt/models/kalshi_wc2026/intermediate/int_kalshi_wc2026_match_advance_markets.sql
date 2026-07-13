with source_markets as (
    select
        m.market_ticker,
        m.event_ticker,
        m.series_ticker,
        m.yes_sub_title,
        m.status,
        m.open_time,
        m.close_time,
        m.expiration_time,
        m.occurrence_datetime,
        trim(
            case
                when right(lower(trim(coalesce(m.yes_sub_title, ''))), 9) = ' advances'
                    then left(trim(m.yes_sub_title), length(trim(m.yes_sub_title)) - 9)
                else trim(coalesce(m.yes_sub_title, ''))
            end
        ) as source_team_name
    from {{ ref('int_kalshi_wc2026_markets') }} as m
    where m.series_ticker = 'KXWCADVANCE'
),

eligible_markets as (
    select
        m.market_ticker,
        m.event_ticker,
        m.series_ticker,
        m.yes_sub_title,
        m.status,
        m.open_time,
        m.close_time,
        m.expiration_time,
        m.occurrence_datetime,
        m.source_team_name,
        coalesce(a.canonical_team_name, m.source_team_name) as canonical_team_name
    from source_markets as m
    left join {{ ref('international_results_wc2026_team_aliases') }} as a
        on lower(m.source_team_name) = lower(a.market_team_name)
    where nullif(m.source_team_name, '') is not null
),

event_pairs as (
    select
        event_ticker,
        min(canonical_team_name) as team_a,
        max(canonical_team_name) as team_b
    from eligible_markets
    group by event_ticker
    having count(*) = 2 and min(canonical_team_name) <> max(canonical_team_name)
),

mapped_events as (
    select
        p.event_ticker,
        f.fifa_match_id,
        f.stage_key,
        f.stage_rank,
        f.kickoff_at_utc,
        f.home_team,
        f.away_team,
        count(*) over (
            partition by f.fifa_match_id
        ) as events_per_fixture
    from event_pairs as p
    inner join {{ ref('int_wc2026_knockout_fixtures') }} as f
        on
            p.team_a = least(f.home_team, f.away_team)
            and p.team_b = greatest(f.home_team, f.away_team)
            and f.teams_resolved
)

select
    e.fifa_match_id,
    e.stage_key,
    e.stage_rank,
    e.kickoff_at_utc,
    e.home_team,
    e.away_team,
    m.event_ticker,
    m.market_ticker,
    m.source_team_name,
    m.canonical_team_name,
    m.status,
    m.open_time,
    m.close_time,
    m.expiration_time,
    m.occurrence_datetime,
    e.events_per_fixture,
    case
        when m.canonical_team_name = e.home_team then 'home'
        when m.canonical_team_name = e.away_team then 'away'
    end as team_side,
    e.events_per_fixture > 1 as is_ambiguous_mapping
from eligible_markets as m
inner join mapped_events as e
    on m.event_ticker = e.event_ticker
where m.canonical_team_name in (e.home_team, e.away_team)
