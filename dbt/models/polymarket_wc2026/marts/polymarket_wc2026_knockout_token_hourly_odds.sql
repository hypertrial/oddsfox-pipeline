with hourly_odds as (
    select
        k.market_id,
        k.outcome_index,
        k.clob_token_id,
        k.question,
        k.source_outcome_label,
        k.event_slug,
        k.market_slug,
        k.condition_id,
        k.sports_market_type,
        k.game_start_time,
        k.group_item_title,
        k.tags,
        k.clob_token_ids,
        k.is_active,
        k.is_closed,
        k.is_resolved,
        k.winning_outcome,
        k.winning_clob_token_id,
        k.market_volume_usd,
        k.stage_key,
        k.stage_rank,
        k.market_direction,
        k.team_name,
        o.odds_timestamp,
        o.odds_timestamp_epoch,
        o.price,
        date_trunc('hour', o.odds_timestamp) as odds_hour_utc
    from {{ ref('polymarket_wc2026_knockout_market_tokens') }} as k
    inner join {{ ref('stg_polymarket_wc2026_odds') }} as o
        on k.clob_token_id = o.clob_token_id
    where
        o.price is not null
        and o.odds_timestamp is not null
        and o.odds_timestamp_epoch is not null
        and o.odds_timestamp >= current_timestamp - interval 30 day
),

ranked as (
    select
        market_id,
        outcome_index,
        clob_token_id,
        question,
        source_outcome_label,
        event_slug,
        market_slug,
        condition_id,
        sports_market_type,
        game_start_time,
        group_item_title,
        tags,
        clob_token_ids,
        is_active,
        is_closed,
        is_resolved,
        winning_outcome,
        winning_clob_token_id,
        market_volume_usd,
        stage_key,
        stage_rank,
        market_direction,
        team_name,
        odds_timestamp,
        odds_timestamp_epoch,
        price,
        odds_hour_utc,
        row_number() over (
            partition by clob_token_id, odds_hour_utc
            order by odds_timestamp_epoch asc, price asc
        ) as open_rank,
        row_number() over (
            partition by clob_token_id, odds_hour_utc
            order by odds_timestamp_epoch desc, price desc
        ) as close_rank
    from hourly_odds
)

select
    market_id,
    outcome_index,
    clob_token_id,
    question,
    source_outcome_label,
    event_slug,
    market_slug,
    condition_id,
    sports_market_type,
    game_start_time,
    group_item_title,
    tags,
    clob_token_ids,
    is_active,
    is_closed,
    is_resolved,
    winning_outcome,
    winning_clob_token_id,
    market_volume_usd,
    stage_key,
    stage_rank,
    market_direction,
    team_name,
    odds_hour_utc,
    cast(epoch(odds_hour_utc) as bigint) as odds_hour_epoch,
    max(case when open_rank = 1 then price end) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    max(case when close_rank = 1 then price end) as close_price,
    round(avg(price), 8) as avg_price,
    count(*) as observed_points,
    min(odds_timestamp_epoch) as first_timestamp,
    min(odds_timestamp) as first_observed_at,
    max(odds_timestamp_epoch) as last_timestamp,
    max(odds_timestamp) as last_observed_at
from ranked
group by
    market_id,
    outcome_index,
    clob_token_id,
    question,
    source_outcome_label,
    event_slug,
    market_slug,
    condition_id,
    sports_market_type,
    game_start_time,
    group_item_title,
    tags,
    clob_token_ids,
    is_active,
    is_closed,
    is_resolved,
    winning_outcome,
    winning_clob_token_id,
    market_volume_usd,
    stage_key,
    stage_rank,
    market_direction,
    team_name,
    odds_hour_utc
