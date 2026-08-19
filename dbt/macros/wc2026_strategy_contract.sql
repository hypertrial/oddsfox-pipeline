{% macro name_match_key(column_expr) -%}
trim(
  regexp_replace(
    regexp_replace(
      lower(strip_accents(cast({{ column_expr }} as varchar))),
      '[^a-z0-9]+',
      ' ',
      'g'
    ),
    '\s+',
    ' ',
    'g'
  )
)
{%- endmacro %}

{% macro canonical_team_match_key(column_expr) -%}
coalesce(
    (
        select aliases.canonical_match_key
        from {{ source("oddsfox_reference", "wc2026_team_canonical_aliases") }} as aliases
        where aliases.variant_match_key = {{ name_match_key(column_expr) }}
        limit 1
    ),
    {{ name_match_key(column_expr) }}
)
{%- endmacro %}

{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
6371.0088 * 2 * asin(
    sqrt(
        power(sin(radians(({{ lat2 }}) - ({{ lat1 }})) / 2), 2)
        + cos(radians({{ lat1 }}))
        * cos(radians({{ lat2 }}))
        * power(sin(radians(({{ lon2 }}) - ({{ lon1 }})) / 2), 2)
    )
)
{%- endmacro %}

{% macro safe_zscore(column_expr) -%}
coalesce(
    ({{ column_expr }} - avg({{ column_expr }}) over ())
    / nullif(stddev_pop({{ column_expr }}) over (), 0),
    0.0
)
{%- endmacro %}
