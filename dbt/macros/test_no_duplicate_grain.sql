{% test no_duplicate_grain(model, grain_columns, where=none) %}
select
    {% for column in grain_columns -%}
    {{ column }}{% if not loop.last %},{% endif %}
    {% endfor -%}
    ,
    count(*) as row_count
from {{ model }}
{% if where is not none -%}
where {{ where }}
{% endif -%}
group by {% for i in range(1, grain_columns | length + 1) -%}
{{ i }}{% if not loop.last %}, {% endif %}
{% endfor -%}
having count(*) > 1
{% endtest %}
