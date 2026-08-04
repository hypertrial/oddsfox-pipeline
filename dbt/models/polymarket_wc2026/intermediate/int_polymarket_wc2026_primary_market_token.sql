with candidates as (
    select
        t.market_id,
        t.outcome_index,
        t.clob_token_id,
        t.token_updated_at,
        t.outcome_label,
        bool_or(lower(t.outcome_label) = 'yes') over (
            partition by t.market_id
        ) as market_has_yes
    from {{ ref('int_polymarket_wc2026_token_working_set') }} as t
    inner join {{ ref('int_polymarket_wc2026_markets') }} as markets
        on t.market_id = markets.market_id
)

select
    market_id,
    outcome_index,
    clob_token_id,
    token_updated_at,
    outcome_label
from candidates
where
    (market_has_yes and lower(outcome_label) = 'yes')
    or not market_has_yes
qualify row_number() over (
    partition by market_id
    order by outcome_index asc, clob_token_id asc
) = 1
