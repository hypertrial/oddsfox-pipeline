with raw_with_classification as (
    select
        market_id,
        volume as market_volume_usd,
        case
            when coalesce(is_resolved, false) then 'resolved'
            when coalesce(is_closed, false) then 'closed'
            when coalesce(is_active, false) then 'live'
            else 'inactive'
        end as market_status,
        coalesce(is_active, false) and coalesce(is_closed, false) as source_state_anomaly,
        case
            when question like 'Will % win the 2026 FIFA World Cup?' then 'winner'
            when question like 'Will % reach the 2026 FIFA World Cup final?' then 'final'
            when question like 'Will % reach the Semifinals at the 2026 FIFA World Cup?' then 'semifinal'
            when question like 'Will % reach the Quarterfinals at the 2026 FIFA World Cup?' then 'quarterfinal'
            when question like 'Will % reach the Round of 16 at the 2026 FIFA World Cup?' then 'round_of_16'
            when question like 'Will % be eliminated in the Round of 16 of the World Cup?' then 'round_of_16'
            when question like 'Will % reach the Round of 32 at the 2026 FIFA World Cup?' then 'round_of_32'
            when question like 'Will % be eliminated in the Round of 32 of the World Cup?' then 'round_of_32'
        end as stage_key,
        case
            when question like 'Will % win the 2026 FIFA World Cup?' then 5
            when question like 'Will % reach the 2026 FIFA World Cup final?' then 4
            when question like 'Will % reach the Semifinals at the 2026 FIFA World Cup?' then 3
            when question like 'Will % reach the Quarterfinals at the 2026 FIFA World Cup?' then 2
            when question like 'Will % reach the Round of 16 at the 2026 FIFA World Cup?' then 1
            when question like 'Will % be eliminated in the Round of 16 of the World Cup?' then 1
            when question like 'Will % reach the Round of 32 at the 2026 FIFA World Cup?' then 0
            when question like 'Will % be eliminated in the Round of 32 of the World Cup?' then 0
        end as stage_rank,
        case
            when question like 'Will % win the 2026 FIFA World Cup?' then 'winner'
            when question like 'Will % be eliminated in the Round of 16 of the World Cup?' then 'elimination'
            when question like 'Will % be eliminated in the Round of 32 of the World Cup?' then 'elimination'
            when question like 'Will % reach %' then 'advance'
        end as market_direction
    from {{ ref('stg_polymarket_wc2026_markets') }}
),

raw_stage as (
    select
        stage_key,
        stage_rank,
        market_direction,
        market_status,
        count(distinct market_id) as raw_classified_markets,
        count(distinct case when coalesce(market_volume_usd, 0) >= 5000 then market_id end)
            as raw_classified_markets_ge_5000,
        count(distinct case when source_state_anomaly then market_id end) as raw_source_state_anomaly_markets,
        min(market_volume_usd) as raw_min_volume_usd,
        max(market_volume_usd) as raw_max_volume_usd
    from raw_with_classification
    where stage_key is not null
    group by 1, 2, 3, 4
),

hourly_by_token as (
    select
        clob_token_id,
        min(odds_hour_utc) as first_hour_utc,
        max(odds_hour_utc) as latest_hour_utc,
        count(*) as hourly_rows
    from {{ ref('polymarket_wc2026_knockout_token_hourly_odds') }}
    group by 1
),

scoped_stage as (
    select
        k.stage_key,
        k.stage_rank,
        k.market_direction,
        k.market_status,
        count(distinct k.market_id) as scoped_markets,
        count(*) as scoped_tokens,
        count(h.clob_token_id) as tokens_with_hourly_odds,
        count(*) - count(h.clob_token_id) as tokens_missing_hourly_odds,
        sum(case when k.source_state_anomaly then 1 else 0 end) as source_state_anomaly_tokens,
        min(k.market_volume_usd) as scoped_min_volume_usd,
        max(k.market_volume_usd) as scoped_max_volume_usd,
        min(h.first_hour_utc) as first_hour_utc,
        max(h.latest_hour_utc) as latest_hour_utc,
        sum(coalesce(h.hourly_rows, 0)) as hourly_rows
    from {{ ref('polymarket_wc2026_knockout_market_tokens') }} as k
    left join hourly_by_token as h
        on k.clob_token_id = h.clob_token_id
    group by 1, 2, 3, 4
)

select
    r.raw_min_volume_usd,
    r.raw_max_volume_usd,
    s.scoped_min_volume_usd,
    s.scoped_max_volume_usd,
    s.first_hour_utc,
    s.latest_hour_utc,
    coalesce(s.stage_key, r.stage_key) as stage_key,
    coalesce(s.market_direction, r.market_direction) as market_direction,
    coalesce(s.market_status, r.market_status) as market_status,
    coalesce(s.stage_rank, r.stage_rank) as stage_rank,
    coalesce(r.raw_classified_markets, 0) as raw_classified_markets,
    coalesce(r.raw_classified_markets_ge_5000, 0) as raw_classified_markets_ge_5000,
    coalesce(r.raw_source_state_anomaly_markets, 0) as raw_source_state_anomaly_markets,
    coalesce(s.scoped_markets, 0) as scoped_markets,
    coalesce(s.scoped_tokens, 0) as scoped_tokens,
    coalesce(s.tokens_with_hourly_odds, 0) as tokens_with_hourly_odds,
    coalesce(s.tokens_missing_hourly_odds, 0) as tokens_missing_hourly_odds,
    coalesce(s.source_state_anomaly_tokens, 0) as source_state_anomaly_tokens,
    coalesce(s.hourly_rows, 0) as hourly_rows,
    current_timestamp as observed_at
from scoped_stage as s
full outer join raw_stage as r
    on
        s.stage_key = r.stage_key
        and s.market_direction = r.market_direction
        and s.market_status = r.market_status
