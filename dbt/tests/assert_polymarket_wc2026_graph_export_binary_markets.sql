{{
    config(
        meta={
            'dagster': {
                'ref': {'name': 'polymarket_wc2026_graph_token_hourly_odds'},
                'asset_key': ['polymarket', 'wc2026', 'marts', 'graph_token_hourly_odds']
            }
        }
    )
}}

with market_tokens as (
    select
        market_id,
        count(distinct clob_token_id) as token_count,
        count(distinct case when lower(outcome_label) = 'yes' then clob_token_id end) as yes_tokens,
        count(distinct case when lower(outcome_label) = 'no' then clob_token_id end) as no_tokens
    from {{ ref('polymarket_wc2026_graph_token_hourly_odds') }}
    group by 1
)

select *
from market_tokens
where token_count != 2 or yes_tokens != 1 or no_tokens != 1
