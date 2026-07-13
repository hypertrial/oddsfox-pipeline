with registry as (
    select distinct market_id
    from {{ source('polymarket_wc2026_ops', 'market_scope_registry') }}
    where lower(scope_name) = 'wc2026'
),

eligible_tokens as (
    select
        t.market_id,
        t.event_slug,
        t.market_slug,
        t.clob_token_id,
        t.outcome_index,
        t.outcome_label as source_team_name,
        t.game_start_time,
        t.is_active,
        t.is_closed,
        t.is_resolved,
        coalesce(a.canonical_team_name, t.outcome_label) as canonical_team_name
    from {{ ref('int_polymarket_wc2026_token_universe') }} as t
    inner join registry
        on t.market_id = registry.market_id
    left join {{ ref('international_results_wc2026_team_aliases') }} as a
        on lower(t.outcome_label) = lower(a.market_team_name)
    where
        lower(coalesce(t.sports_market_type, '')) = 'soccer_team_to_advance'
        and nullif(trim(t.outcome_label), '') is not null
),

market_pairs as (
    select
        market_id,
        event_slug,
        min(canonical_team_name) as team_a,
        max(canonical_team_name) as team_b
    from eligible_tokens
    group by market_id, event_slug
    having count(*) = 2 and min(canonical_team_name) <> max(canonical_team_name)
),

mapped_markets as (
    select
        p.market_id,
        p.event_slug,
        f.fifa_match_id,
        f.stage_key,
        f.stage_rank,
        f.kickoff_at_utc,
        f.home_team,
        f.away_team,
        count(*) over (
            partition by f.fifa_match_id
        ) as markets_per_fixture
    from market_pairs as p
    inner join {{ ref('int_wc2026_knockout_fixtures') }} as f
        on
            p.team_a = least(f.home_team, f.away_team)
            and p.team_b = greatest(f.home_team, f.away_team)
            and f.teams_resolved
)

select
    m.fifa_match_id,
    m.stage_key,
    m.stage_rank,
    m.kickoff_at_utc,
    m.home_team,
    m.away_team,
    t.market_id,
    t.event_slug,
    t.market_slug,
    t.clob_token_id,
    t.outcome_index,
    t.source_team_name,
    t.canonical_team_name,
    t.game_start_time,
    t.is_active,
    t.is_closed,
    t.is_resolved,
    m.markets_per_fixture,
    case
        when t.canonical_team_name = m.home_team then 'home'
        when t.canonical_team_name = m.away_team then 'away'
    end as team_side,
    m.markets_per_fixture > 1 as is_ambiguous_mapping
from eligible_tokens as t
inner join mapped_markets as m
    on t.market_id = m.market_id
where t.canonical_team_name in (m.home_team, m.away_team)
