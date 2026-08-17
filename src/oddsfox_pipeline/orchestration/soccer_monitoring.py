"""Local production monitoring for the Polymarket soccer scope."""

from __future__ import annotations

import asyncio
import json
import os
import resource
import shutil
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Thread
from typing import Any, Iterator

from oddsfox_pipeline.config import settings
from oddsfox_pipeline.naming import SCOPE_SOCCER
from oddsfox_pipeline.orchestration.dbt_project import DBT_PROJECT
from oddsfox_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    get_connection,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
)
from oddsfox_pipeline.storage.minute_odds_snapshots import minute_odds_snapshot_root

_TERMINAL_STEPS = {
    "polymarket_soccer_market_scope_registry_refresh": "match_result_registry",
    "polymarket_soccer_match_result_minute_odds_ingest": "match_minute_odds",
    "polymarket_soccer_dbt_build": "dbt_build",
    "polymarket_soccer_full_pipeline": "dbt_build",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, default=str, sort_keys=True)


def _rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _directories, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                continue
    return total


def resource_diagnostics(*, started_at: float, started_cpu: float) -> dict[str, Any]:
    """Return dependency-free process and local-storage diagnostics."""
    elapsed = max(0.001, time.monotonic() - started_at)
    cpu_seconds = max(0.0, time.process_time() - started_cpu)
    warehouse = active_duckdb_path()
    usage = shutil.disk_usage(warehouse.parent)
    wal = Path(f"{warehouse}.wal")
    runtime_root = Path(os.getenv("ODDSFOX_RUNTIME_ROOT", ".cache/runtime"))
    temporary_roots = {runtime_root / "duckdb-temp", runtime_root / "tmp"}
    return {
        "elapsed_seconds": round(elapsed, 3),
        "process_cpu_seconds": round(cpu_seconds, 3),
        "process_cpu_percent": round(cpu_seconds / elapsed * 100.0, 2),
        "peak_rss_bytes": _rss_bytes(),
        "warehouse_bytes": warehouse.stat().st_size if warehouse.exists() else 0,
        "wal_bytes": wal.stat().st_size if wal.exists() else 0,
        "temporary_bytes": sum(_directory_bytes(path) for path in temporary_roots),
        "disk_free_bytes": usage.free,
    }


def _prune_monitoring_history(conn, *, now: datetime) -> None:
    cutoff = now - timedelta(days=settings.POLYMARKET_SOCCER_MONITOR_HISTORY_DAYS)
    runs = polymarket_ops_tbl(SCOPE_SOCCER, "pipeline_runs")
    steps = polymarket_ops_tbl(SCOPE_SOCCER, "pipeline_step_runs")
    old_runs = f"""
        SELECT dagster_run_id FROM {runs}
        WHERE finished_at < ? AND status <> 'running'
        QUALIFY row_number() OVER (
            PARTITION BY job_name, status ORDER BY finished_at DESC
        ) > 1
    """
    conn.execute(f"DELETE FROM {steps} WHERE dagster_run_id IN ({old_runs})", [cutoff])
    conn.execute(f"DELETE FROM {runs} WHERE dagster_run_id IN ({old_runs})", [cutoff])


class SoccerStepMonitor:
    def __init__(self, context: Any, step_name: str) -> None:
        self.context = context
        self.step_name = step_name
        self.run_id = str(context.run_id)
        self.job_name = str(context.job_name)
        self.attempt = int(getattr(context, "retry_number", 0) or 0)
        self.started = _utcnow()
        self.started_monotonic = time.monotonic()
        self.started_cpu = time.process_time()
        self._status = "success"
        self._metrics: dict[str, Any] = {}

    def start(self) -> None:
        runs = polymarket_ops_tbl(SCOPE_SOCCER, "pipeline_runs")
        steps = polymarket_ops_tbl(SCOPE_SOCCER, "pipeline_step_runs")
        with get_connection() as conn:
            _prune_monitoring_history(conn, now=self.started)
            conn.execute(
                f"""
                INSERT INTO {runs} (
                    dagster_run_id, job_name, started_at, heartbeat_at, status
                ) VALUES (?, ?, ?, ?, 'running')
                ON CONFLICT (dagster_run_id) DO UPDATE SET
                    heartbeat_at = excluded.heartbeat_at,
                    status = 'running'
                """,
                [self.run_id, self.job_name, self.started, self.started],
            )
            conn.execute(
                f"""
                INSERT INTO {steps} (
                    dagster_run_id, step_name, attempt_number, phase,
                    started_at, heartbeat_at, status
                ) VALUES (?, ?, ?, 'start', ?, ?, 'running')
                ON CONFLICT (dagster_run_id, step_name, attempt_number) DO UPDATE SET
                    phase = 'start', heartbeat_at = excluded.heartbeat_at,
                    finished_at = NULL, status = 'running', error_type = NULL,
                    error_message = NULL
                """,
                [self.run_id, self.step_name, self.attempt, self.started, self.started],
            )

    def complete(self, metrics: dict[str, Any] | None = None) -> None:
        if metrics:
            self._metrics.update(metrics)
        status = str(self._metrics.get("status") or "success").lower()
        self._status = "partial" if status == "partial" else "success"

    def finish(self, exc: BaseException | None = None) -> None:
        finished = _utcnow()
        metrics = dict(self._metrics)
        metrics.update(
            resource_diagnostics(
                started_at=self.started_monotonic, started_cpu=self.started_cpu
            )
        )
        error_type = None
        error_message = None
        status = self._status
        if exc is not None:
            status = (
                "interrupted"
                if isinstance(
                    exc,
                    (GeneratorExit, KeyboardInterrupt, asyncio.CancelledError),
                )
                or "interrupt" in exc.__class__.__name__.lower()
                or "cancel" in exc.__class__.__name__.lower()
                else "failed"
            )
            error_type = exc.__class__.__name__
            error_message = str(exc)[:500]
        runs = polymarket_ops_tbl(SCOPE_SOCCER, "pipeline_runs")
        steps = polymarket_ops_tbl(SCOPE_SOCCER, "pipeline_step_runs")
        with get_connection() as conn:
            if exc is None and self.step_name == "dbt_build":
                try:
                    quality = conn.execute(
                        """
                        SELECT mapping_coverage_percent,
                            observed_minute_coverage_percent,
                            dense_minute_coverage_percent,
                            observed_minutes, dense_minutes
                        FROM polymarket_soccer_observability
                            .polymarket_soccer_match_result_data_quality
                        """
                    ).fetchone()
                    if quality is not None:
                        metrics.update(
                            {
                                "mapping_coverage_percent": (
                                    float(quality[0])
                                    if quality[0] is not None
                                    else None
                                ),
                                "observed_minute_coverage_percent": (
                                    float(quality[1])
                                    if quality[1] is not None
                                    else None
                                ),
                                "dense_minute_coverage_percent": (
                                    float(quality[2])
                                    if quality[2] is not None
                                    else None
                                ),
                                "observed_minutes": (
                                    int(quality[3]) if quality[3] is not None else None
                                ),
                                "dense_minutes": (
                                    int(quality[4]) if quality[4] is not None else None
                                ),
                            }
                        )
                except Exception:
                    # The terminal dbt step still records its resource metrics when
                    # an older or deliberately reduced model selection omits quality.
                    pass
            conn.execute(
                f"""
                UPDATE {steps} SET phase = 'complete', heartbeat_at = ?,
                    finished_at = ?, status = ?, error_type = ?, error_message = ?,
                    metrics_json = ?
                WHERE dagster_run_id = ? AND step_name = ? AND attempt_number = ?
                """,
                [
                    finished,
                    finished,
                    status,
                    error_type,
                    error_message,
                    _json(metrics),
                    self.run_id,
                    self.step_name,
                    self.attempt,
                ],
            )
            is_terminal = _TERMINAL_STEPS.get(self.job_name) == self.step_name
            if status in {"failed", "interrupted"} or is_terminal:
                run_status = status
                if is_terminal and status == "success":
                    has_partial = conn.execute(
                        f"""
                        SELECT count(*) FROM {steps}
                        WHERE dagster_run_id = ? AND status = 'partial'
                        """,
                        [self.run_id],
                    ).fetchone()[0]
                    run_status = "partial" if has_partial else "success"
                conn.execute(
                    f"""
                    UPDATE {runs} SET heartbeat_at = ?, finished_at = ?, status = ?,
                        terminal_step = ?, metrics_json = ?
                    WHERE dagster_run_id = ?
                    """,
                    [
                        finished,
                        finished,
                        run_status,
                        self.step_name,
                        _json(metrics),
                        self.run_id,
                    ],
                )
                try:
                    alert_rows = conn.execute(
                        """
                        SELECT alert_code, severity, subject, measured_value,
                            threshold_value, message
                        FROM polymarket_soccer_observability.polymarket_soccer_pipeline_alerts
                        """
                    ).fetchall()
                    warning_count = sum(row[1] == "warning" for row in alert_rows)
                    critical_count = sum(row[1] == "critical" for row in alert_rows)
                    logger = getattr(self.context, "log", None)
                    if logger is not None:
                        for row in alert_rows:
                            logger.warning(
                                "soccer_pipeline_alert %s",
                                _json(
                                    dict(
                                        zip(
                                            (
                                                "alert_code",
                                                "severity",
                                                "subject",
                                                "measured_value",
                                                "threshold_value",
                                                "message",
                                            ),
                                            row,
                                            strict=True,
                                        )
                                    )
                                ),
                            )
                except Exception:
                    warning_count = critical_count = 0
                conn.execute(
                    f"""
                    UPDATE {runs} SET warning_count = ?, critical_count = ?
                    WHERE dagster_run_id = ?
                    """,
                    [warning_count, critical_count, self.run_id],
                )
            else:
                conn.execute(
                    f"UPDATE {runs} SET heartbeat_at = ? WHERE dagster_run_id = ?",
                    [finished, self.run_id],
                )


@contextmanager
def monitor_soccer_step(context: Any, step_name: str) -> Iterator[SoccerStepMonitor]:
    monitor = SoccerStepMonitor(context, step_name)
    monitor.start()
    stopped = Event()

    def _heartbeat() -> None:
        while not stopped.wait(60):
            context.log.info(
                "soccer_monitoring_heartbeat %s",
                _json(
                    {
                        "dagster_run_id": monitor.run_id,
                        "job_name": monitor.job_name,
                        "step_name": step_name,
                        **resource_diagnostics(
                            started_at=monitor.started_monotonic,
                            started_cpu=monitor.started_cpu,
                        ),
                    }
                ),
            )

    heartbeat = Thread(target=_heartbeat, daemon=True)
    heartbeat.start()
    try:
        yield monitor
    except BaseException as exc:
        stopped.set()
        heartbeat.join(timeout=1)
        monitor.finish(exc)
        raise
    else:
        stopped.set()
        heartbeat.join(timeout=1)
        monitor.finish()


def run_soccer_preflight() -> dict[str, Any]:
    """Validate local contracts before any external request is made."""
    warehouse = active_duckdb_path()
    free_bytes = shutil.disk_usage(warehouse.parent).free
    critical_bytes = int(settings.POLYMARKET_SOCCER_MONITOR_DISK_CRITICAL_GIB * 1024**3)
    if free_bytes < critical_bytes:
        raise RuntimeError(
            f"soccer preflight requires {critical_bytes} free bytes; found {free_bytes}"
        )

    snapshot_root = minute_odds_snapshot_root(leg="match", scope_name=SCOPE_SOCCER)
    snapshot_root.mkdir(parents=True, exist_ok=True)
    probe = snapshot_root / f".preflight-{os.getpid()}"
    probe.touch(exist_ok=False)
    probe.unlink()

    manifest_path = DBT_PROJECT.manifest_path
    try:
        manifest = json.loads(manifest_path.read_text())
        model_names = {
            str(node.get("name"))
            for node in manifest.get("nodes", {}).values()
            if node.get("resource_type") == "model"
        }
    except (OSError, ValueError, TypeError) as exc:
        raise RuntimeError(f"soccer dbt manifest is unreadable: {exc}") from exc
    required_models = {
        "polymarket_soccer_matches",
        "polymarket_soccer_match_result_minute_odds",
        "polymarket_soccer_pipeline_health",
    }
    missing_models = sorted(required_models - model_names)
    if missing_models:
        raise RuntimeError(f"soccer dbt manifest models missing: {missing_models}")

    payloads = polymarket_raw_tbl(SCOPE_SOCCER, "event_market_payload_snapshots")
    registry = polymarket_ops_tbl(SCOPE_SOCCER, "match_result_registry")
    with get_connection() as conn:
        bootstrap_polymarket_tables(conn, scope_name=SCOPE_SOCCER)
        actual = {
            str(item[0])
            for item in conn.execute(f"SELECT * FROM {payloads} LIMIT 0").description
        }
        required = {
            name
            for name in EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS
            if name != "row_order"
        }
        missing = sorted(required - actual)
        if missing:
            raise RuntimeError(f"soccer market snapshot columns missing: {missing}")
        conn.execute(
            f"""
            SELECT market_id, observed_at, scraped_at FROM {payloads}
            QUALIFY row_number() OVER (
                PARTITION BY market_id ORDER BY observed_at DESC, scraped_at DESC
            ) = 1 LIMIT 0
            """
        )
        collision_count = conn.execute(
            f"""
            SELECT count(*) - count(DISTINCT token_id)
            FROM (
                SELECT unnest([yes_token_id, no_token_id]) AS token_id
                FROM {registry}
            )
            """
        ).fetchone()[0]
        if collision_count:
            raise RuntimeError(
                f"soccer registry contains {collision_count} cross-side token collisions"
            )
    return {
        "status": "success",
        "disk_free_bytes": free_bytes,
        "disk_warning": free_bytes
        < int(settings.POLYMARKET_SOCCER_MONITOR_DISK_WARN_GIB * 1024**3),
        "snapshot_root": str(snapshot_root),
        "warehouse": str(warehouse),
        "dbt_manifest": str(manifest_path),
    }


__all__ = [
    "monitor_soccer_step",
    "resource_diagnostics",
    "run_soccer_preflight",
]
