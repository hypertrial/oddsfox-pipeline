with contract as (
    select *
    from {{ ref('polymarket_wc2026_contract') }}
    where scope_name = 'wc2026'
),

raw_stage as (
    select
        c.stage_key,
        c.stage_rank,
        c.market_direction,
        c.market_status,
        count(distinct c.market_id) as raw_classified_markets,
        count(
            distinct case
                when coalesce(c.market_volume_usd, 0) >= contract.knockout_min_volume_usd
                    then c.market_id
            end
        ) as raw_classified_markets_ge_floor,
        count(distinct case when c.source_state_anomaly then c.market_id end)
            as raw_source_state_anomaly_markets,
        min(c.market_volume_usd) as raw_min_volume_usd,
        max(c.market_volume_usd) as raw_max_volume_usd
    from {{ ref('int_polymarket_wc2026_knockout_market_classification') }} as c
    -- costguard: allow cross-join, WC2026 contract seed has one row.
    cross join contract
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
        sum(coalesce(h.hourly_rows, 0)) as hourly_rows,
        count(*) * max(contract.hourly_window_hours) as expected_hourly_rows,
        round(sum(coalesce(h.hourly_rows, 0))::double / nullif(count(*), 0), 4)
            as avg_hourly_rows_per_token,
        min(coalesce(h.hourly_rows, 0)) as min_hourly_rows_per_token,
        max(coalesce(h.hourly_rows, 0)) as max_hourly_rows_per_token,
        least(
            round(
                sum(coalesce(h.hourly_rows, 0))::double
                / nullif(count(*) * max(contract.hourly_window_hours), 0),
                6
            ),
            1.0
        ) as hourly_completeness_ratio
    from {{ ref('polymarket_wc2026_knockout_market_tokens') }} as k
    left join hourly_by_token as h
        on k.clob_token_id = h.clob_token_id
    -- costguard: allow cross-join, WC2026 contract seed has one row.
    cross join contract
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
    coalesce(r.raw_classified_markets_ge_floor, 0) as raw_classified_markets_ge_floor,
    coalesce(r.raw_source_state_anomaly_markets, 0) as raw_source_state_anomaly_markets,
    coalesce(s.scoped_markets, 0) as scoped_markets,
    coalesce(s.scoped_tokens, 0) as scoped_tokens,
    coalesce(s.tokens_with_hourly_odds, 0) as tokens_with_hourly_odds,
    coalesce(s.tokens_missing_hourly_odds, 0) as tokens_missing_hourly_odds,
    coalesce(s.source_state_anomaly_tokens, 0) as source_state_anomaly_tokens,
    coalesce(s.hourly_rows, 0) as hourly_rows,
    coalesce(s.expected_hourly_rows, 0) as expected_hourly_rows,
    coalesce(s.avg_hourly_rows_per_token, 0) as avg_hourly_rows_per_token,
    coalesce(s.min_hourly_rows_per_token, 0) as min_hourly_rows_per_token,
    coalesce(s.max_hourly_rows_per_token, 0) as max_hourly_rows_per_token,
    coalesce(s.hourly_completeness_ratio, 0) as hourly_completeness_ratio,
    current_timestamp as observed_at
from scoped_stage as s
full outer join raw_stage as r
    on
        s.stage_key = r.stage_key
        and s.market_direction = r.market_direction
        and s.market_status = r.market_status
