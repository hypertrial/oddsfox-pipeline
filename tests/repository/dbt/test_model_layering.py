from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_check


DBT_ROOT = Path(__file__).resolve().parents[3] / "dbt"


def test_staging_markets_is_source_conformed():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "staging"
        / "stg_polymarket_wc2026_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "event_market_payload_snapshots" in lowered
    assert "market_scope_registry" not in lowered
    assert "market_scope_event_slugs" not in lowered
    assert "is_market_scope_target" not in lowered


def test_staging_odds_filters_to_payload_market_tokens():
    """Odds/ledger/skips/daily staging must stay inside payload-backed tokens."""
    staging = DBT_ROOT / "models" / "polymarket_wc2026" / "staging"
    for name in (
        "stg_polymarket_wc2026_odds.sql",
        "stg_polymarket_wc2026_odds_daily.sql",
        "stg_polymarket_wc2026_sync_ledger.sql",
        "stg_polymarket_wc2026_token_sync_skips.sql",
    ):
        lowered = (staging / name).read_text().lower()
        assert "ref('stg_polymarket_wc2026_market_tokens')" in lowered
        assert "inner join" in lowered


def test_intermediate_wc2026_markets_owns_scope_logic():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "intermediate"
        / "int_polymarket_wc2026_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "ref('stg_polymarket_wc2026_markets')" in lowered
    assert "source('polymarket_wc2026_ops', 'market_scope_registry')" in lowered
    assert "ref('int_polymarket_wc2026_event_latest')" in lowered
    assert "ref('stg_polymarket_wc2026_event_market_snapshots')" in lowered
    assert "is_event_volume_eligible" in lowered
    assert "is_enclosing_event" in lowered
    assert "'wc2026'" in lowered
    assert "knockout_min_volume_usd" not in lowered


def test_wc2026_hourly_fact_aggregates_canonical_odds_directly():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "intermediate"
        / "int_polymarket_wc2026_token_hourly_odds.sql"
    ).read_text()
    macro_sql = (DBT_ROOT / "macros" / "polymarket_models.sql").read_text()
    lowered = sql.lower()
    lowered_macro = macro_sql.lower()

    assert "polymarket_token_hourly_odds_sql(" in lowered
    assert "ref('stg_polymarket_wc2026_odds')" in lowered
    assert "date_trunc('hour', o.odds_timestamp)" in lowered_macro
    assert "latest_ingested_at" in lowered_macro
    assert "is_incremental()" in lowered_macro
    assert "{{ ref('polymarket_wc2026_token_hourly_odds') }}" not in lowered
    assert "hourly_window_days" not in lowered
    assert "hourly_window_days" not in lowered_macro
    assert "selected_" not in lowered


def test_wc2026_market_hourly_odds_mart_joins_fact_to_market_metadata():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "marts"
        / "polymarket_wc2026_market_hourly_odds.sql"
    ).read_text()
    lowered = sql.lower()
    primary_sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "intermediate"
        / "int_polymarket_wc2026_primary_market_token.sql"
    ).read_text()
    primary_lowered = primary_sql.lower()

    assert "{{ ref('int_polymarket_wc2026_primary_market_token') }}" in lowered
    assert "{{ ref('int_polymarket_wc2026_token_hourly_odds') }}" in lowered
    assert "{{ ref('int_polymarket_wc2026_markets') }}" in lowered
    assert "{{ ref('polymarket_wc2026_token_hourly_odds') }}" not in lowered
    assert "selected_" not in lowered
    assert "primary_outcome_label" in lowered
    assert "market_has_yes" in primary_lowered
    assert "not market_has_yes" in primary_lowered
    # Must not hard-filter only to yes as the sole gate (fallback exists).
    assert "or not market_has_yes" in primary_lowered
