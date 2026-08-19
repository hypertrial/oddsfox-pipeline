"""Coverage for the shared disposable dbt CI DuckDB bootstrap."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from tests.unit.storage.duckdb_storage_test_support import isolate_duckdb_test_env


def _load_bootstrap_module():
    path = (
        Path(__file__).resolve().parents[3] / "scripts" / "bootstrap_dbt_ci_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("bootstrap_dbt_ci_duckdb", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bootstrap_dbt_ci_duckdb_seeds_expected_schemas(monkeypatch, tmp_path):
    db_path = tmp_path / "bootstrap.duckdb"
    isolate_duckdb_test_env(monkeypatch, db_path)
    module = _load_bootstrap_module()

    path = module.bootstrap_dbt_ci_duckdb()
    assert path == db_path.resolve() or path == db_path

    import oddsfox_pipeline.storage.duckdb.connection as connection

    with connection.get_connection() as conn:
        schemas = {
            row[0]
            for row in conn.execute(
                "select schema_name from information_schema.schemata"
            ).fetchall()
        }
        assert "polymarket_wc2026_raw" in schemas
        assert "kalshi_wc2026_raw" in schemas
        polymarket_events = conn.execute(
            "select count(*) from polymarket_wc2026_ops.ingestion_run_events"
        ).fetchone()[0]
        kalshi_events = conn.execute(
            "select count(*) from kalshi_wc2026_ops.ingestion_run_events"
        ).fetchone()[0]
    assert polymarket_events >= 1
    assert kalshi_events >= 1
