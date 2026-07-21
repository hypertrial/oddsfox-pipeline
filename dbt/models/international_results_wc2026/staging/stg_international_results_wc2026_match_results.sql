select
    match_id,
    cast(match_date as date) as match_date,
    home_team,
    away_team,
    cast(home_score as integer) as home_score,
    cast(away_score as integer) as away_score,
    tournament,
    city,
    country,
    cast(neutral as boolean) as neutral,
    match_status,
    source_url,
    source_row_number,
    source_row_hash,
    source_revision,
    source_payload_sha256,
    source_loaded_at,
    case
        when match_date between date '2026-06-11' and date '2026-06-27' then 'group_stage'
        when match_date between date '2026-06-28' and date '2026-07-03' then 'round_of_32'
        when match_date between date '2026-07-04' and date '2026-07-07' then 'round_of_16'
        when match_date between date '2026-07-09' and date '2026-07-11' then 'quarterfinal'
        when match_date between date '2026-07-14' and date '2026-07-15' then 'semifinal'
        when match_date = date '2026-07-18' then 'third_place'
        when match_date = date '2026-07-19' then 'final'
    end as stage_key,
    case
        when match_date between date '2026-06-11' and date '2026-06-27' then 0
        when match_date between date '2026-06-28' and date '2026-07-03' then 1
        when match_date between date '2026-07-04' and date '2026-07-07' then 2
        when match_date between date '2026-07-09' and date '2026-07-11' then 3
        when match_date between date '2026-07-14' and date '2026-07-15' then 4
        when match_date = date '2026-07-18' then 5
        when match_date = date '2026-07-19' then 6
    end as stage_rank,
    match_date > date '2026-06-27' as is_knockout
from {{ source('international_results_wc2026_raw', 'match_results') }}
