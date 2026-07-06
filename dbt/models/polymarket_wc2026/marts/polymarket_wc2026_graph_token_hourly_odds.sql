-- costguard: disable-file=SQLCOST038
-- Graph export intentionally joins hourly fact rows to one-row-per-token
-- semantics on clob_token_id; odds_hour_epoch stays on the fact side.
with binary_markets as (
    select
        market_id,
        max(
            case when lower(outcome_label) = 'yes' then clob_token_id end
        ) as yes_clob_token_id,
        max(
            case when lower(outcome_label) = 'no' then clob_token_id end
        ) as no_clob_token_id
    from {{ ref('int_polymarket_wc2026_market_tokens') }}
    group by 1
    having
        count(*) = 2
        and sum(case when lower(outcome_label) = 'yes' then 1 else 0 end) = 1
        and sum(case when lower(outcome_label) = 'no' then 1 else 0 end) = 1
),

token_pairs as (
    select
        t.market_id,
        t.outcome_index,
        t.clob_token_id,
        t.outcome_label,
        case
            when lower(t.outcome_label) = 'yes' then b.no_clob_token_id
            else b.yes_clob_token_id
        end as opposite_clob_token_id
    from {{ ref('int_polymarket_wc2026_market_tokens') }} as t
    inner join binary_markets as b
        on t.market_id = b.market_id
    where lower(t.outcome_label) in ('yes', 'no')
),

classified_tokens as (
    select
        p.market_id,
        p.outcome_index,
        p.clob_token_id,
        c.question,
        p.outcome_label,
        c.event_slug,
        c.is_active,
        c.is_closed,
        c.market_volume_usd,
        c.stage_key,
        c.stage_rank,
        c.canonical_team_name,
        c.market_direction,
        c.progression_outcome_label,
        p.opposite_clob_token_id,
        c.market_status,
        c.is_still_alive,
        case
            when c.market_direction in ('winner', 'advance') and lower(p.outcome_label) = 'yes' then true
            when c.market_direction = 'elimination' and lower(p.outcome_label) = 'no' then true
            else false
        end as is_progression_token
    from token_pairs as p
    inner join {{ ref('int_polymarket_wc2026_knockout_market_classification') }} as c
        on p.market_id = c.market_id
)

select
    c.market_id,
    c.outcome_index,
    c.clob_token_id,
    c.question,
    c.outcome_label,
    c.event_slug,
    c.is_active,
    c.is_closed,
    c.market_volume_usd,
    c.stage_key,
    c.stage_rank,
    c.canonical_team_name,
    c.market_direction,
    c.progression_outcome_label,
    c.is_progression_token,
    c.opposite_clob_token_id,
    c.market_status,
    c.is_still_alive,
    h.odds_hour_utc,
    h.odds_hour_epoch,
    h.open_price,
    h.high_price,
    h.low_price,
    h.close_price,
    h.avg_price,
    h.observed_points,
    h.first_timestamp,
    h.first_observed_at,
    h.last_timestamp,
    h.last_observed_at
from {{ ref('int_polymarket_wc2026_token_hourly_odds') }} as h
inner join classified_tokens as c
    on h.clob_token_id = c.clob_token_id
