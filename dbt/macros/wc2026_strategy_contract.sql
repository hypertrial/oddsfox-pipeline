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
        from {{ ref("wc2026_team_canonical_aliases") }} as aliases
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

{% macro ensure_wc2026_canonical_raw_tables() %}
  {% if execute %}
    {% do run_query("create schema if not exists wc2026_raw") %}
    {% do run_query("create schema if not exists wc2026_ops") %}
    {% do run_query(
      "create table if not exists wc2026_ops.raw_snapshot_ledger (
        source varchar,
        snapshot_id varchar,
        collected_at timestamptz,
        collector_git_sha varchar,
        collector_container_digest varchar,
        manifest_sha256 varchar,
        loaded_at timestamptz default current_timestamp
      )"
    ) %}
    {% do run_query(
      "create table if not exists wc2026_raw.eloratings__team_ratings (
        rank integer, team_code varchar, team_name varchar, rating double,
        snapshot_year integer, snapshot_scope varchar,
        _source varchar, _snapshot_id varchar, _collected_at timestamptz
      )"
    ) %}
    {% do run_query(
      "create table if not exists wc2026_raw.clubelo__club_ratings (
        snapshot_date date, club_key varchar, club_name varchar,
        api_club_name varchar, country_code varchar, elo double, rank integer,
        valid_from date, valid_to date,
        _source varchar, _snapshot_id varchar, _collected_at timestamptz
      )"
    ) %}
    {% do run_query(
      "create table if not exists wc2026_raw.fifaindex__players (
        game_slug varchar, competition_key varchar, player_id bigint,
        player_name varchar, nationality varchar, positions varchar,
        primary_position varchar, overall double, age double, pace double,
        shooting double, passing_rating double, dribbling double,
        defending double, physical double, gk_diving double,
        gk_handling double, gk_kicking double, gk_positioning double,
        gk_reflexes double, club varchar, league varchar, player_gender varchar,
        was_world_cup_squad_member boolean, world_cup_squad_team varchar,
        world_cup_squad_tournament_year integer,
        _source varchar, _snapshot_id varchar, _collected_at timestamptz
      )"
    ) %}
    {% do run_query(
      "create table if not exists wc2026_raw.wikipedia_squads__players (
        source_player_key varchar, run_id varchar,
        official_wc2026_squad_team varchar, source_team_code varchar,
        official_wc2026_player_name varchar,
        official_wc2026_squad_position varchar,
        official_wc2026_squad_number integer,
        official_wc2026_squad_club varchar,
        official_wc2026_squad_dob date,
        official_wc2026_squad_group varchar,
        official_wc2026_squad_coach varchar,
        official_wc2026_squad_caps integer,
        official_wc2026_squad_goals integer,
        _source varchar, _snapshot_id varchar, _collected_at timestamptz
      )"
    ) %}
    {% do run_query(
      "create table if not exists wc2026_raw.fotmob__events (
        match_id varchar, event_id varchar, event_type varchar,
        event_minute integer, event_second integer, team varchar,
        player varchar, event_at timestamptz, source_status varchar,
        _source varchar, _snapshot_id varchar, _collected_at timestamptz
      )"
    ) %}
  {% endif %}
{% endmacro %}
