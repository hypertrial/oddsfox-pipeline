from pathlib import Path

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"


def test_staging_markets_is_source_conformed():
    sql = (
        DBT_ROOT / "models" / "polymarket" / "staging" / "stg_polymarket_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "market_scope_registry" not in lowered
    assert "market_scope_event_slugs" not in lowered
    assert "is_market_scope_target" not in lowered


def test_intermediate_selected_markets_owns_scope_logic():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket"
        / "intermediate"
        / "int_polymarket_selected_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('stg_polymarket_markets') }}" in lowered
    assert "{{ source('polymarket_ops', 'market_scope_registry') }}" in lowered
    assert "active_market_scopes" in lowered
    assert "scope_name" in lowered
    assert "market_scope_event_slugs" not in lowered


def test_selected_minutely_filters_before_odds_join():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket"
        / "marts"
        / "selected_token_minutely_odds.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('int_polymarket_selected_token_universe') }}" in lowered
    assert "{{ ref('stg_polymarket_odds') }}" in lowered
    assert "int_polymarket_token_timeseries" not in lowered


def test_selected_hourly_aggregates_canonical_odds():
    sql = (
        DBT_ROOT / "models" / "polymarket" / "marts" / "selected_token_hourly_odds.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('int_polymarket_selected_token_universe') }}" in lowered
    assert "{{ ref('stg_polymarket_odds') }}" in lowered
    assert "date_trunc('hour', odds_timestamp)" in lowered
    assert "selected_whale_hourly_odds" not in lowered


def test_selected_live_hourly_filters_historical_hourly_scope():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket"
        / "marts"
        / "selected_token_live_hourly_odds.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('selected_token_hourly_odds') }}" in lowered
    assert "{{ ref('stg_polymarket_odds') }}" not in lowered
    assert "{{ ref('int_polymarket_selected_token_universe') }}" not in lowered
    for column in (
        "market_id",
        "outcome_index",
        "clob_token_id",
        "odds_hour_utc",
        "odds_hour_epoch",
        "last_observed_at",
    ):
        assert f"h.{column}" in lowered
    assert "count(distinct h.clob_token_id) = t.expected_tokens" in lowered
    assert "max(odds_hour_epoch) as current_hour_epoch" in lowered
    assert "global_current_hour_epoch" in lowered
    assert "polymarket_live_current_max_age_hours" in lowered
    assert "bool_or(coalesce(h.is_active, false))" in lowered
    assert "not bool_or(coalesce(h.is_closed, false))" in lowered
