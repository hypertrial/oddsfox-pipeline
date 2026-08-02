{{ config(
    meta = {
        'dagster': {
            'ref': {'name': 'polymarket_wc2026_logical_propositions'},
            'asset_key': ['polymarket', 'wc2026', 'marts', 'logical_propositions']
        }
    }
) }}

select
    source_proposition_id,
    polarity,
    event_constraint_kind
from {{ ref('polymarket_wc2026_logical_propositions') }}
where
    event_constraint_group_id is not null
    and (polarity != 'positive' or event_constraint_kind != 'at_most_one')
