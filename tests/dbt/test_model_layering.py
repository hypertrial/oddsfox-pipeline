from pathlib import Path

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"


def test_staging_markets_is_source_conformed():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "staging"
        / "stg_polymarket_wc2026_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "market_scope_registry" not in lowered
    assert "market_scope_event_slugs" not in lowered
    assert "is_market_scope_target" not in lowered


def test_intermediate_wc2026_markets_owns_scope_logic():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "intermediate"
        / "int_polymarket_wc2026_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('stg_polymarket_wc2026_markets') }}" in lowered
    assert "{{ source('polymarket_wc2026_ops', 'market_scope_registry') }}" in lowered
    assert "active_market_scopes" not in lowered
    assert "where lower(scope_name) = 'wc2026'" in lowered
    assert "scope_name" in lowered
    assert "market_scope_event_slugs" not in lowered


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
    assert "ref('polymarket_wc2026_contract')" in lowered
    assert "date_trunc('hour', o.odds_timestamp)" in lowered_macro
    assert "latest_ingested_at" in lowered_macro
    assert "is_incremental()" in lowered_macro
    assert "{{ ref('polymarket_wc2026_token_hourly_odds') }}" not in lowered
    assert "hourly_window_days" in lowered_macro
    assert "selected_" not in lowered


def test_wc2026_knockout_hourly_view_joins_current_metadata_to_hourly_fact():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "marts"
        / "polymarket_wc2026_knockout_token_hourly_odds.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('polymarket_wc2026_knockout_market_tokens') }}" in lowered
    assert "{{ ref('int_polymarket_wc2026_token_hourly_odds') }}" in lowered
    assert "{{ ref('polymarket_wc2026_token_hourly_odds') }}" not in lowered
    assert "selected_" not in lowered


def test_wc2026_graph_hourly_view_keeps_binary_market_tokens():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "marts"
        / "polymarket_wc2026_graph_token_hourly_odds.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('int_polymarket_wc2026_market_tokens') }}" in lowered
    assert (
        "{{ ref('int_polymarket_wc2026_knockout_market_classification') }}" in lowered
    )
    assert "{{ ref('int_polymarket_wc2026_token_hourly_odds') }}" in lowered
    assert "is_progression_token" in lowered
    assert "opposite_clob_token_id" in lowered
    assert "lower(p.outcome_label) = 'yes'" in lowered
    assert "lower(p.outcome_label) = 'no'" in lowered


def test_wc2026_knockout_classifier_is_shared_intermediate():
    token_sql = (
        (
            DBT_ROOT
            / "models"
            / "polymarket_wc2026"
            / "marts"
            / "polymarket_wc2026_knockout_market_tokens.sql"
        )
        .read_text()
        .lower()
    )
    coverage_sql = (
        (
            DBT_ROOT
            / "models"
            / "polymarket_wc2026"
            / "observability"
            / "polymarket_wc2026_knockout_stage_coverage.sql"
        )
        .read_text()
        .lower()
    )

    classifier_ref = "{{ ref('int_polymarket_wc2026_knockout_market_classification') }}"
    assert classifier_ref in token_sql
    assert classifier_ref in coverage_sql


def test_wc2026_knockout_snapshot_keeps_historical_rows_with_status():
    sql = (
        DBT_ROOT
        / "models"
        / "polymarket_wc2026"
        / "marts"
        / "polymarket_wc2026_knockout_markets.sql"
    ).read_text()
    lowered = sql.lower()

    assert "{{ ref('polymarket_wc2026_knockout_market_tokens') }}" in lowered
    assert "left join current_token_prices" in lowered
    assert "market_status" in lowered
    assert "current_price_status" in lowered
    assert "where market_status = 'live'" not in lowered
    assert "where is_live_market" not in lowered
