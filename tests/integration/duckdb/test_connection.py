import duckdb

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.storage.duckdb.connection import init_duck_db


def test_init_duck_db_creates_polymarket_schemas_only(tmp_path, monkeypatch):
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
                where table_schema in ('wc2026_polymarket_raw', 'wc2026_polymarket_ops')
                """
            ).fetchall()
        }

    assert schemas == {"wc2026_polymarket_raw", "wc2026_polymarket_ops"}
    assert {
        ("wc2026_polymarket_raw", "market_tokens"),
        ("wc2026_polymarket_raw", "odds_history"),
        ("wc2026_polymarket_raw", "token_odds_daily"),
        ("wc2026_polymarket_ops", "market_scope_registry"),
        ("wc2026_polymarket_ops", "token_sync_ledger"),
        ("wc2026_polymarket_ops", "token_sync_skips"),
        ("wc2026_polymarket_ops", "pipeline_run_events"),
        ("wc2026_polymarket_ops", "sync_run_metrics"),
    } <= tables
    assert ("wc2026_polymarket_raw", "markets") not in tables
