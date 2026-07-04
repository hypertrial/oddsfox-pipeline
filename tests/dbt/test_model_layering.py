from pathlib import Path

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"


def test_staging_markets_is_source_conformed():
    sql = (
        DBT_ROOT
        / "models"
        / "wc2026_polymarket"
        / "staging"
        / "stg_wc2026_polymarket_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "market_scope_registry" not in lowered
    assert "market_scope_event_slugs" not in lowered
    assert "is_market_scope_target" not in lowered


def test_intermediate_wc2026_markets_owns_scope_logic():
    sql = (
        DBT_ROOT
        / "models"
        / "wc2026_polymarket"
        / "intermediate"
        / "int_wc2026_polymarket_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('stg_wc2026_polymarket_markets') }}" in lowered
    assert "{{ source('wc2026_polymarket_ops', 'market_scope_registry') }}" in lowered
    assert "active_market_scopes" not in lowered
    assert "where lower(scope_name) = 'wc2026'" in lowered
    assert "scope_name" in lowered
    assert "market_scope_event_slugs" not in lowered


def test_wc2026_hourly_aggregates_canonical_odds():
    sql = (
        DBT_ROOT
        / "models"
        / "wc2026_polymarket"
        / "marts"
        / "wc2026_token_hourly_odds.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('int_wc2026_polymarket_market_tokens') }}" in lowered
    assert "{{ ref('stg_wc2026_polymarket_odds') }}" in lowered
    assert "date_trunc('hour', odds_timestamp)" in lowered
    assert "selected_" not in lowered
