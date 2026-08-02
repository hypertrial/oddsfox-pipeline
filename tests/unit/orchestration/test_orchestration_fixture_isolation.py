"""Regression coverage for opt-in orchestration DuckDB setup."""

from __future__ import annotations

from pathlib import Path


def test_mocked_orchestration_test_does_not_create_duckdb(tmp_path: Path, monkeypatch):
    """Autouse guards must stay cheap when storage is mocked."""
    db_path = tmp_path / "should-not-exist.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    from oddsfox_pipeline.orchestration import polymarket_ops as polymarket_ops_mod

    assert polymarket_ops_mod.sync_markets()["task"] == "sync_markets"
    assert not db_path.exists()


def test_orchestration_duckdb_fixture_initializes_schemas(orchestration_duckdb):
    with orchestration_duckdb.get_connection() as conn:
        schemas = {
            row[0]
            for row in conn.execute(
                "select schema_name from information_schema.schemata"
            ).fetchall()
        }
    assert "polymarket_wc2026_raw" in schemas
    assert "kalshi_wc2026_raw" in schemas
