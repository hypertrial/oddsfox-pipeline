import duckdb

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.storage.duckdb.connection import init_duck_db


def test_init_duck_db_creates_raw_and_ops_schemas(tmp_path, monkeypatch):
    db_path = tmp_path / "oddsfox.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()

    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        schemas = {
            row[0]
            for row in conn.execute(
                """
                select schema_name
                from information_schema.schemata
                where schema_name not in ('information_schema', 'main', 'pg_catalog')
                """
            ).fetchall()
        }
        tables = {
            (row[0], row[1])
            for row in conn.execute(
                """
                select table_schema, table_name
                from information_schema.tables
                where table_schema in (
                    'polymarket_wc2026_raw',
                    'polymarket_wc2026_ops',
                    'polymarket_us_midterms_2026_raw',
                    'polymarket_us_midterms_2026_ops',
                    'kalshi_wc2026_raw',
                    'kalshi_wc2026_ops',
                    'international_results_wc2026_raw',
                    'openfootball_wc2026_raw'
                )
                """
            ).fetchall()
        }

    assert schemas == {
        "polymarket_wc2026_raw",
        "polymarket_wc2026_ops",
        "polymarket_us_midterms_2026_raw",
        "polymarket_us_midterms_2026_ops",
        "kalshi_wc2026_raw",
        "kalshi_wc2026_ops",
        "international_results_wc2026_raw",
        "openfootball_wc2026_raw",
    }
    assert {
        ("international_results_wc2026_raw", "match_results"),
        ("openfootball_wc2026_raw", "knockout_fixtures"),
        ("polymarket_us_midterms_2026_raw", "market_tokens"),
        ("polymarket_us_midterms_2026_raw", "odds_history"),
        ("polymarket_us_midterms_2026_ops", "market_scope_registry"),
        ("polymarket_wc2026_raw", "market_tokens"),
        ("polymarket_wc2026_raw", "odds_history"),
        ("polymarket_wc2026_raw", "token_odds_daily"),
        ("polymarket_wc2026_ops", "market_scope_registry"),
        ("polymarket_wc2026_ops", "token_sync_ledger"),
        ("polymarket_wc2026_ops", "token_sync_skips"),
        ("kalshi_wc2026_ops", "market_scope_registry"),
        ("kalshi_wc2026_ops", "candlestick_sync_ledger"),
        ("kalshi_wc2026_ops", "pipeline_run_events"),
        ("kalshi_wc2026_ops", "sync_run_metrics"),
        ("kalshi_wc2026_raw", "market_candlesticks_hourly"),
        ("polymarket_wc2026_ops", "sync_run_metrics"),
    } <= tables
    assert ("polymarket_wc2026_raw", "markets") not in tables
