from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace

import duckdb
import pytest

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.orchestration import assets_soccer
from oddsfox_pipeline.orchestration.assets_soccer import (
    polymarket_soccer_production_health_check,
)
from oddsfox_pipeline.orchestration.soccer_monitoring import (
    monitor_soccer_step,
    record_soccer_check_failure,
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


def test_blocking_check_failure_overwrites_terminal_success(tmp_path, monkeypatch):
    db_path = tmp_path / "check_failure.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    context = SimpleNamespace(
        run_id="run-check",
        job_name="polymarket_soccer_dbt_build",
        retry_number=0,
    )
    with monitor_soccer_step(context, "dbt_build") as monitor:
        monitor.complete()

    record_soccer_check_failure(
        run_id="run-check", check_name="minute_mart_contracts_valid", metadata={}
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            "select status, terminal_step from polymarket_soccer_ops.pipeline_runs"
        ).fetchone() == ("failed", "asset_check:minute_mart_contracts_valid")
        assert conn.execute(
            "select status from polymarket_soccer_ops.pipeline_step_runs "
            "where step_name = 'asset_check:minute_mart_contracts_valid'"
        ).fetchone() == ("failed",)


def test_blocking_check_failure_uses_asset_check_run(monkeypatch):
    recorded = {}
    monkeypatch.setattr(
        assets_soccer,
        "record_soccer_check_failure",
        lambda **kwargs: recorded.update(kwargs),
    )

    result = assets_soccer._blocking_check_result(
        SimpleNamespace(run=SimpleNamespace(run_id="run-check")),
        name="minute_mart_contracts_valid",
        passed=False,
        metadata={"invalid_spines": 1},
    )

    assert not result.passed
    assert recorded == {
        "run_id": "run-check",
        "check_name": "minute_mart_contracts_valid",
        "metadata": {"invalid_spines": 1},
    }


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


def test_soccer_step_heartbeat_persists_liveness(tmp_path, monkeypatch):
    db_path = tmp_path / "heartbeat.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    context = SimpleNamespace(
        run_id="run-heartbeat",
        job_name="polymarket_soccer_full_pipeline",
        retry_number=0,
    )
    with monitor_soccer_step(context, "event_catalog") as monitor:
        with duckdb.connect(str(db_path)) as conn:
            conn.execute(
                "update polymarket_soccer_ops.pipeline_runs "
                "set heartbeat_at = timestamp '2000-01-01'"
            )
            conn.execute(
                "update polymarket_soccer_ops.pipeline_step_runs "
                "set heartbeat_at = timestamp '2000-01-01'"
            )
        monitor.heartbeat()
        with duckdb.connect(str(db_path), read_only=True) as conn:
            assert conn.execute(
                "select min(heartbeat_at) > timestamp '2000-01-01' "
                "from polymarket_soccer_ops.pipeline_runs"
            ).fetchone() == (True,)
            assert conn.execute(
                "select min(heartbeat_at) > timestamp '2000-01-01' "
                "from polymarket_soccer_ops.pipeline_step_runs"
            ).fetchone() == (True,)


def test_soccer_preflight_rejects_corrupt_event_projection(tmp_path, monkeypatch):
    db_path = tmp_path / "preflight.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    assert run_soccer_preflight()["status"] == "success"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("alter table polymarket_soccer_raw.events drop event_title")

    with pytest.raises(RuntimeError, match="current events schema mismatch"):
        run_soccer_preflight()


def test_production_health_check_fails_on_critical_state(tmp_path, monkeypatch):
    db_path = tmp_path / "health.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    assert run_soccer_preflight()["status"] == "success"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("create schema polymarket_soccer_observability")
        conn.execute(
            "create view polymarket_soccer_observability."
            "polymarket_soccer_pipeline_health as select 'critical' health_status, "
            "1::bigint critical_count, 0::bigint warning_count, "
            "'running' latest_run_status"
        )

    result = (
        polymarket_soccer_production_health_check.node_def.compute_fn.decorated_fn()
    )

    assert not result.passed
    assert result.metadata["critical_alerts"].value == 1


def test_alert_history_preserves_first_observation(tmp_path, monkeypatch):
    db_path = tmp_path / "alerts.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    connection.reset_duckdb_connection_state()
    assert run_soccer_preflight()["status"] == "success"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("create schema polymarket_soccer_observability")
        conn.execute("create table active_alert(first_at timestamp, last_at timestamp)")
        conn.execute(
            "insert into active_alert values "
            "(timestamp '2026-01-01', timestamp '2026-01-02')"
        )
        conn.execute(
            "create view polymarket_soccer_observability."
            "polymarket_soccer_pipeline_alerts as select 'retry' alert_code, "
            "'warning' severity, 'token' subject, '1' measured_value, "
            "'0' threshold_value, 'retry token' message, first_at first_observed_at, "
            "last_at last_observed_at from active_alert"
        )
    for run_id in ("alert-1", "alert-2"):
        context = SimpleNamespace(
            run_id=run_id,
            job_name="polymarket_soccer_dbt_build",
            retry_number=0,
        )
        with monitor_soccer_step(context, "dbt_build") as monitor:
            monitor.complete()
        if run_id == "alert-1":
            with duckdb.connect(str(db_path)) as conn:
                conn.execute(
                    "update active_alert set first_at = timestamp '2026-02-01', "
                    "last_at = timestamp '2026-02-02'"
                )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            "select first_observed_at, last_observed_at "
            "from polymarket_soccer_ops.pipeline_alert_history"
        ).fetchone() == (
            datetime(2026, 1, 1),
            datetime(2026, 2, 2),
        )


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
