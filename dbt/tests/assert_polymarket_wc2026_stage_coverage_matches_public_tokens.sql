with token_counts as (
    select
        stage_key,
        market_direction,
        market_status,
        count(distinct market_id) as scoped_markets,
        count(*) as scoped_tokens
    from {{ ref('polymarket_wc2026_knockout_market_tokens') }}
    group by 1, 2, 3
),

coverage_counts as (
    select
        stage_key,
        market_direction,
        market_status,
        scoped_markets,
        scoped_tokens
    from {{ ref('polymarket_wc2026_knockout_stage_coverage') }}
)

select
    coalesce(t.stage_key, c.stage_key) as stage_key,
    coalesce(t.market_direction, c.market_direction) as market_direction,
    coalesce(t.market_status, c.market_status) as market_status,
    coalesce(t.scoped_markets, 0) as token_scoped_markets,
    coalesce(c.scoped_markets, 0) as coverage_scoped_markets,
    coalesce(t.scoped_tokens, 0) as token_scoped_tokens,
    coalesce(c.scoped_tokens, 0) as coverage_scoped_tokens
from token_counts as t
full outer join coverage_counts as c
    on
        t.stage_key = c.stage_key
        and t.market_direction = c.market_direction
        and t.market_status = c.market_status
where
    coalesce(t.scoped_markets, 0) != coalesce(c.scoped_markets, 0)
    or coalesce(t.scoped_tokens, 0) != coalesce(c.scoped_tokens, 0)
