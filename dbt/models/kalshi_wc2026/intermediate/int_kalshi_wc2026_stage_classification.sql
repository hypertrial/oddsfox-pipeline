with scoped as (
    select
        m.market_ticker,
        m.event_ticker,
        m.series_ticker,
        m.event_suffix,
        m.market_suffix,
        m.title,
        m.subtitle,
        m.yes_sub_title,
        m.no_sub_title,
        m.status,
        m.market_type,
        m.open_time,
        m.close_time,
        m.expiration_time,
        m.volume as market_volume,
        m.open_interest,
        m.last_price,
        m.scraped_at,
        m.scope_name,
        coalesce(nullif(m.yes_sub_title, ''), nullif(m.title, '')) as team_name,
        case
            when m.series_ticker = 'KXMENWORLDCUP' then 'winner'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'GS' then 'group_stage'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'R32' then 'round_of_32'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'R16' then 'round_of_16'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'QF' then 'quarterfinal'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'SF' then 'semifinal'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'FL' then 'final'
        end as stage_key,
        case
            when m.series_ticker = 'KXMENWORLDCUP' then 6
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'GS' then 0
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'R32' then 1
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'R16' then 2
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'QF' then 3
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'SF' then 4
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'FL' then 5
        end as stage_rank,
        case
            when m.series_ticker = 'KXMENWORLDCUP' then 'winner'
            when m.series_ticker = 'KXWCSTAGEOFELIM' then 'elimination'
        end as market_direction,
        case
            when m.series_ticker = 'KXMENWORLDCUP' then 'win_world_cup'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'GS'
                then 'not_eliminated_in_group_stage'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'R32'
                then 'not_eliminated_in_round_of_32'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'R16'
                then 'not_eliminated_in_round_of_16'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'QF'
                then 'reach_quarterfinal'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'SF'
                then 'reach_semifinal'
            when m.series_ticker = 'KXWCSTAGEOFELIM' and m.market_suffix = 'FL'
                then 'reach_final'
        end as progression_outcome_label
    from {{ ref('int_kalshi_wc2026_markets') }} as m
    where m.series_ticker in ('KXMENWORLDCUP', 'KXWCSTAGEOFELIM')
),

team_scoped as (
    select
        c.market_ticker,
        c.event_ticker,
        c.series_ticker,
        c.event_suffix,
        c.market_suffix,
        c.title,
        c.subtitle,
        c.yes_sub_title,
        c.no_sub_title,
        c.status,
        c.market_type,
        c.open_time,
        c.close_time,
        c.expiration_time,
        c.market_volume,
        c.open_interest,
        c.last_price,
        c.scraped_at,
        c.scope_name,
        c.team_name,
        c.stage_key,
        c.stage_rank,
        c.market_direction,
        c.progression_outcome_label,
        'progression' as price_represents,
        ts.team_name as canonical_team_name,
        ts.tournament_status,
        ts.is_still_alive,
        ts.eliminated_stage_key,
        ts.eliminated_match_date,
        ts.next_match_date,
        ts.next_stage_key,
        ts.matches_played,
        ts.wins,
        ts.draws,
        ts.losses,
        ts.goals_for,
        ts.goals_against,
        ts.latest_completed_match_date,
        ts.latest_completed_stage_key
    from scoped as c
    left join {{ ref('international_results_wc2026_team_aliases') }} as a
        on lower(c.team_name) = lower(a.market_team_name)
    inner join {{ ref('international_results_wc2026_team_status') }} as ts
        on lower(coalesce(a.canonical_team_name, c.team_name)) = lower(ts.team_name)
    where c.stage_key is not null
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
    false as source_state_anomaly,
    case
        when lower(status) = 'finalized' then 'resolved'
        when lower(status) = 'active' then 'live'
        else 'inactive'
    end as market_status,
    lower(status) = 'active' as is_live_market
from team_scoped
