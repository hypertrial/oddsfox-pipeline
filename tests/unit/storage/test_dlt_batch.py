from __future__ import annotations

import importlib

import pytest

from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.storage.duckdb import dlt_batch as dlt_batch_mod
from oddsfox.storage.duckdb.dlt_batch import (
    load_market_tokens_stage,
    load_stage_rows,
    reset_dlt_batch_pipelines,
)
from oddsfox.storage.duckdb.schemas.constants import polymarket_raw_tbl


@pytest.fixture
def duck(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "dlt-batch.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    reset_dlt_batch_pipelines()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()
    yield connection
    reset_dlt_batch_pipelines()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False


def test_dlt_batch_loads_stage_and_finalizes_market_tokens(duck):
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            CREATE TABLE {polymarket_raw_tbl("stage_market_tokens")} (
                market_id TEXT,
                clob_token_ids TEXT,
                updated_at TIMESTAMP
            )
            """
        )
        load_market_tokens_stage(
            [
                {
                    "market_id": "m1",
                    "clobTokenIds": '["tok-a"]',
                    "updated_at": "2026-01-01T00:00:00",
                }
            ],
            conn,
        )
        canonical = conn.execute(
            f"""
            SELECT market_id, clobTokenIds
            FROM {polymarket_raw_tbl("market_tokens")}
            """
        ).fetchall()
        staged = conn.execute(
            f"""
            SELECT market_id, clob_token_ids
            FROM {polymarket_raw_tbl("stage_market_tokens_v1")}
            """
        ).fetchall()

    assert canonical == [("m1", '["tok-a"]')]
    assert staged == canonical


def test_load_stage_rows_rejects_empty_rows():
    with pytest.raises(ValueError, match="rows must not be empty"):
        load_stage_rows(
            schema="polymarket_raw", stage_table="stage", rows=[], columns={}
        )


def test_load_stage_rows_drops_pending_packages(monkeypatch):
    class Pipe:
        has_pending_data = True

        def __init__(self):
            self.dropped = False
            self.runs = []

        def drop_pending_packages(self):
            self.dropped = True

        def run(self, rows, **kwargs):
            self.runs.append((rows, kwargs))

    pipe = Pipe()
    monkeypatch.setattr(dlt_batch_mod, "_pipeline", lambda _schema: pipe)

    stage = load_stage_rows(
        schema="polymarket_raw",
        stage_table="stage_probe",
        rows=[{"id": "1"}],
        columns={"id": {"data_type": "text"}},
    )

    assert pipe.dropped is True
    assert pipe.runs
    assert stage == '"polymarket_raw"."stage_probe"'
