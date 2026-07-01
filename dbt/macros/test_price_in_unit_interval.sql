{% test price_in_unit_interval(model, price_column='price', identifier_columns=none) %}
select
    {% if identifier_columns is not none -%}
    {% for column in identifier_columns -%}
    {{ column }},
    {% endfor -%}
    {% endif -%}
    {{ price_column }} as price
from {{ model }}
where
    {{ price_column }} is not null
    and ({{ price_column }} < 0 or {{ price_column }} > 1)
{% endtest %}
