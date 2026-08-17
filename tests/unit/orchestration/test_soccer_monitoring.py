from __future__ import annotations

import json
from types import SimpleNamespace

import duckdb

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.orchestration.soccer_monitoring import (
    monitor_soccer_step,
    resource_diagnostics,
    run_soccer_preflight,
)


def test_soccer_step_ledger_preserves_partial_full_run(tmp_path, monkeypatch):
    db_path = tmp_path / "monitor.duckdb"
    runtime_root = tmp_path / "runtime"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(runtime_root))
    connection.reset_duckdb_connection_state()
    context = SimpleNamespace(
        run_id="run-1",
        job_name="polymarket_soccer_full_pipeline",
        retry_number=0,
    )

    assert run_soccer_preflight()["status"] == "success"
    with monitor_soccer_step(context, "match_minute_odds") as monitor:
        monitor.complete({"status": "partial", "error_tokens": 1})
    with monitor_soccer_step(context, "dbt_build") as monitor:
        monitor.complete({"status": "success"})

    with duckdb.connect(str(db_path), read_only=True) as conn:
        run = conn.execute(
            """
            select status, terminal_step
            from polymarket_soccer_ops.pipeline_runs
            where dagster_run_id = 'run-1'
            """
        ).fetchone()
        steps = conn.execute(
            """
            select step_name, status
            from polymarket_soccer_ops.pipeline_step_runs
            order by step_name
            """
        ).fetchall()

    assert run == ("partial", "dbt_build")
    assert steps == [("dbt_build", "success"), ("match_minute_odds", "partial")]


def test_soccer_step_ledger_records_failure(tmp_path, monkeypatch):
    db_path = tmp_path / "monitor_failure.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    context = SimpleNamespace(
        run_id="run-failed",
        job_name="polymarket_soccer_match_result_minute_odds_ingest",
        retry_number=2,
    )

    try:
        with monitor_soccer_step(context, "match_minute_odds"):
            raise RuntimeError("clob unavailable")
    except RuntimeError:
        pass

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            """
            select status, error_type, attempt_number
            from polymarket_soccer_ops.pipeline_step_runs
            """
        ).fetchone() == ("failed", "RuntimeError", 2)
        assert conn.execute(
            "select status from polymarket_soccer_ops.pipeline_runs"
        ).fetchone() == ("failed",)


def test_soccer_step_ledger_records_cancellation(tmp_path, monkeypatch):
    db_path = tmp_path / "cancelled.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    context = SimpleNamespace(
        run_id="run-cancelled",
        job_name="polymarket_soccer_match_result_minute_odds_ingest",
        retry_number=0,
    )

    try:
        with monitor_soccer_step(context, "match_minute_odds"):
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        pass

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            "select status from polymarket_soccer_ops.pipeline_runs"
        ).fetchone() == ("interrupted",)


def test_soccer_resource_diagnostics_are_nonnegative(tmp_path, monkeypatch):
    db_path = tmp_path / "resources.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()

    metrics = resource_diagnostics(started_at=0.0, started_cpu=0.0)

    assert metrics["elapsed_seconds"] > 0
    assert metrics["process_cpu_seconds"] >= 0
    assert metrics["peak_rss_bytes"] > 0
    assert metrics["disk_free_bytes"] > 0


def test_soccer_terminal_dbt_step_records_publication_quality(tmp_path, monkeypatch):
    db_path = tmp_path / "quality.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    assert run_soccer_preflight()["status"] == "success"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("create schema polymarket_soccer_observability")
        conn.execute(
            """
            create table polymarket_soccer_observability
                .polymarket_soccer_match_result_data_quality as
            select 25.0 as mapping_coverage_percent,
                40.0 as observed_minute_coverage_percent,
                100.0 as dense_minute_coverage_percent,
                12::bigint as observed_minutes, 30::bigint as dense_minutes
            """
        )
    context = SimpleNamespace(
        run_id="run-quality",
        job_name="polymarket_soccer_dbt_build",
        retry_number=0,
    )

    with monitor_soccer_step(context, "dbt_build") as monitor:
        monitor.complete()

    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw_metrics = conn.execute(
            """
            select metrics_json from polymarket_soccer_ops.pipeline_step_runs
            where dagster_run_id = 'run-quality' and step_name = 'dbt_build'
            """
        ).fetchone()[0]
    metrics = json.loads(raw_metrics)
    assert metrics["observed_minute_coverage_percent"] == 40.0
    assert metrics["dense_minute_coverage_percent"] == 100.0
