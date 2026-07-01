{% test price_columns_in_unit_interval(model, price_columns, identifier_columns=none) %}
select
    {% if identifier_columns is not none -%}
    {% for column in identifier_columns -%}
    {{ column }},
    {% endfor -%}
    {% endif -%}
    {% for column in price_columns -%}
    {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ model }}
where
    {% for column in price_columns -%}
    ({{ column }} is not null and ({{ column }} < 0 or {{ column }} > 1))
    {%- if not loop.last %} or {% endif %}
    {% endfor %}
{% endtest %}
