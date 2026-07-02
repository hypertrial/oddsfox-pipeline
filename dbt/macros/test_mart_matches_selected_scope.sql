{% test mart_matches_selected_scope(
    model,
    int_timeseries,
    token_universe,
    grain_key_column,
    value_columns,
    expected_where,
    mismatch_type,
    include_values_in_output=false
) %}
with expected as (
    select
        t.clob_token_id,
        ts.{{ grain_key_column }},
        {% for column in value_columns -%}
        ts.{{ column }}{% if not loop.last %},{% endif %}
        {% endfor -%}
    from {{ ref(int_timeseries) }} as ts
    inner join {{ ref(token_universe) }} as t
        on ts.clob_token_id = t.clob_token_id
    where {{ expected_where }}
)

select
    'mart_only' as failure_type,
    m.clob_token_id,
    m.{{ grain_key_column }}
    {% if include_values_in_output -%}
    {% for column in value_columns -%}
    , m.{{ column }}
    {% endfor -%}
    {% endif -%}
from {{ model }} as m
left join expected as e
    on
        m.clob_token_id = e.clob_token_id
        and m.{{ grain_key_column }} = e.{{ grain_key_column }}
where e.clob_token_id is null

union all

select
    'expected_only' as failure_type,
    e.clob_token_id,
    e.{{ grain_key_column }}
    {% if include_values_in_output -%}
    {% for column in value_columns -%}
    , e.{{ column }}
    {% endfor -%}
    {% endif -%}
from expected as e
left join {{ model }} as m
    on
        e.clob_token_id = m.clob_token_id
        and e.{{ grain_key_column }} = m.{{ grain_key_column }}
where m.clob_token_id is null

union all

select
    '{{ mismatch_type }}' as failure_type,
    m.clob_token_id,
    m.{{ grain_key_column }}
    {% if include_values_in_output -%}
    {% for column in value_columns -%}
    , m.{{ column }}
    {% endfor -%}
    {% endif -%}
from {{ model }} as m
inner join expected as e
    on
        m.clob_token_id = e.clob_token_id
        and m.{{ grain_key_column }} = e.{{ grain_key_column }}
where
    {% for column in value_columns -%}
    m.{{ column }} is distinct from e.{{ column }}{% if not loop.last %}
    or {% endif %}
    {% endfor -%}
{% endtest %}
