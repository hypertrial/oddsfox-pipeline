{{ config(alias='results') }}

with fixtures as (
    select
        fixture.*,
        venue.host_city
    from {{ ref('wc2026_fixtures') }} as fixture
    left join {{ ref('wc2026_base_camp_venues') }} as venue
        on fixture.venue = venue.venue
)

select
    f.match_id,
    r.home_score,
    r.away_score,
    r.source_url as source_provenance,
    r.source_loaded_at as observed_at,
    case
        when not f.is_knockout then null
        when r.home_score > r.away_score then r.home_team
        when r.away_score > r.home_score then r.away_team
        else r.advancing_team
    end as winner_team,
    case
        when r.match_status = 'completed' then 'final'
        else r.match_status
    end as status
from fixtures as f
inner join {{ ref('international_results_wc2026_matches') }} as r
    on
        f.match_date = r.match_date
        and (
            (
                {{ canonical_team_match_key('f.home_team') }}
                = {{ canonical_team_match_key('r.home_team') }}
                and {{ canonical_team_match_key('f.away_team') }}
                = {{ canonical_team_match_key('r.away_team') }}
            )
            or (
                f.is_knockout
                and {{ name_match_key('f.host_city') }} = {{ name_match_key('r.city') }}
            )
        )
where r.match_status = 'completed'
