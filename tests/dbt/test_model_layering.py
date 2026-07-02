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
    assert "active_market_scope" in lowered
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
