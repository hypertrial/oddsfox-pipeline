{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_logical_propositions'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'logical_propositions']
        }
    }
) }}

with expected as (
    select
        market_id,
        json_array_length(outcomes) as expected_count
    from {{ ref('int_polymarket_wc2026_logical_markets') }}
    where
        parsed_outcomes is not null
        and json_type(parsed_outcomes) = 'ARRAY'
),

actual as (
    select
        market_id,
        count(*) as actual_count
    from {{ ref('polymarket_wc2026_logical_propositions') }}
    group by market_id
)

select
    expected.market_id,
    expected.expected_count,
    coalesce(actual.actual_count, 0) as actual_count
from expected
left join actual on expected.market_id = actual.market_id
where expected.expected_count != coalesce(actual.actual_count, 0)
