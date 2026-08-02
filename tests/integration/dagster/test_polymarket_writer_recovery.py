"""Writer-flush recovery for Polymarket WC2026 odds ingest (no real dbt)."""

from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from tests.integration.dagster.test_scope_refresh_e2e import (
    _materialize_refresh_path,
    _polymarket_business_state,
)

import oddsfox_pipeline.storage.duckdb.connection as connection


def test_refresh_path_recovers_after_second_writer_flush_failure(
    monkeypatch,
    tmp_path,
    reset_connection_globals,
    no_sleep,
) -> None:
    slug = "world-cup-2026-writer-recovery"
    question = "Will the World Cup 2026 writer recovery pass?"
    db_name = "pipeline-writer-recovery.duckdb"
    failed_db = _materialize_refresh_path(
        monkeypatch,
        tmp_path,
        db_name=db_name,
        slug=slug,
        question=question,
        transient_token=None,
        fail_second_writer_flush=True,
        one_point_history=True,
        run_dbt=False,
    )
    with connection.get_connection() as conn:
        assert conn.execute(
            'select count(*) from "polymarket_wc2026_raw"."odds_history"'
        ).fetchone() == (1,)
        assert conn.execute(
            'select count(*) from "polymarket_wc2026_ops"."token_sync_ledger"'
        ).fetchone() == (0,)
        assert conn.execute(
            'select count(*) from "polymarket_wc2026_raw"."token_odds_daily"'
        ).fetchone() == (0,)

    _materialize_refresh_path(
        monkeypatch,
        tmp_path,
        db_name=db_name,
        slug=slug,
        question=question,
        transient_token=None,
        one_point_history=True,
        run_dbt=False,
    )
    with connection.get_connection() as conn:
        recovered_state = {
            k: v
            for k, v in _polymarket_business_state(conn, scope_name="wc2026").items()
            if k != "hourly_model"
        }

    clean_dir = tmp_path / "clean"
    clean_dir.mkdir()
    _materialize_refresh_path(
        monkeypatch,
        clean_dir,
        db_name="pipeline-writer-clean.duckdb",
        slug=slug,
        question=question,
        transient_token=None,
        one_point_history=True,
        run_dbt=False,
    )
    with connection.get_connection() as conn:
        clean_state = {
            k: v
            for k, v in _polymarket_business_state(conn, scope_name="wc2026").items()
            if k != "hourly_model"
        }

    assert failed_db.exists()
    assert recovered_state == clean_state
