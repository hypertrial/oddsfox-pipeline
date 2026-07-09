"""Storage tests for Kalshi schema bootstrap and indexes."""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb

from oddsfox_pipeline.storage.duckdb.schemas import kalshi as kalshi_schema
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    kalshi_ops_tbl,
)


def test_create_test_kalshi_raw_tables_and_seed_pipeline_event(duck):
    with duck.get_connection() as conn:
        kalshi_schema.create_test_kalshi_raw_tables(conn)
        kalshi_schema.create_all_kalshi_test_raw_tables(conn)
        kalshi_schema.seed_test_kalshi_pipeline_run_event(conn)
        row = conn.execute(
            f"""
            SELECT task_name
            FROM {kalshi_ops_tbl("wc2026", "pipeline_run_events")}
            WHERE task_name = 'sync_kalshi_candlesticks'
            """
        ).fetchone()
    assert row is not None


def test_ensure_kalshi_indexes_when_raw_tables_exist():
    with duckdb.connect(":memory:") as conn:
        conn.execute('CREATE SCHEMA IF NOT EXISTS "kalshi_wc2026_ops"')
        conn.execute('CREATE SCHEMA IF NOT EXISTS "kalshi_wc2026_raw"')
        kalshi_schema.bootstrap_kalshi_tables(conn)
        kalshi_schema.create_test_kalshi_raw_tables(conn)
        kalshi_schema.ensure_kalshi_indexes(conn)
        kalshi_schema.ensure_all_kalshi_indexes(conn)

        rows = conn.execute(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE schema_name = 'kalshi_wc2026_raw'
              AND table_name IN ('markets', 'events')
            """
        ).fetchall()
        assert rows


def test_ensure_kalshi_indexes_swallows_index_errors(monkeypatch):
    bad = MagicMock()

    def _execute_side_effect(sql, *args, **kwargs):
        if "information_schema.tables" in str(sql):
            result = MagicMock()
            result.fetchone.return_value = (1,)
            return result
        raise RuntimeError("no index")

    bad.execute.side_effect = _execute_side_effect
    kalshi_schema.ensure_kalshi_indexes(bad)
    assert bad.execute.called
