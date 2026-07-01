from pathlib import Path

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"


def test_staging_markets_is_source_conformed():
    sql = (
        DBT_ROOT / "models" / "polymarket" / "staging" / "stg_polymarket_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "wc2026_market_registry" not in lowered
    assert "wc2026_event_slugs" not in lowered
    assert "is_wc2026_target" not in lowered


def test_intermediate_wc2026_markets_owns_scope_logic():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket"
        / "intermediate"
        / "int_polymarket_wc2026_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('stg_polymarket_markets') }}" in lowered
    assert "{{ source('polymarket_ops', 'wc2026_market_registry') }}" in lowered
    assert "wc2026_event_slugs" in lowered
    assert "true as is_wc2026_target" in lowered
