#!/usr/bin/env python3
"""Summarize recent ingestion run health from DuckDB ops tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

_OPS_TABLES = (
    ("polymarket", "wc2026", "polymarket_wc2026_ops"),
    ("polymarket", "soccer", "polymarket_soccer_ops"),
    ("kalshi", "wc2026", "kalshi_wc2026_ops"),
)


def _load_metrics(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _status_label(metrics: dict[str, Any]) -> str:
    status = str(metrics.get("status") or "success").strip().lower()
    if status in {"failed", "error", "fetch_failed", "audit_error", "preflight_error"}:
        return "failed"
    if metrics.get("aborted") is True:
        return "aborted"
    if metrics.get("skipped") is True:
        return "skipped"
    if metrics.get("noop") is True:
        return "noop"
    return status or "success"


def _format_row(
    *,
    source: str,
    scope: str,
    task_name: str,
    recorded_at: Any,
    metrics: dict[str, Any],
) -> str:
    status = _status_label(metrics)
    error = metrics.get("error_type")
    suffix = f" error={error}" if error else ""
    return f"{recorded_at}  {source}/{scope}  {task_name:<28}  {status:<8}{suffix}"


def _valid_soccer_health(health: dict[str, Any]) -> bool:
    warning_count = health.get("warning_count")
    critical_count = health.get("critical_count")
    status = health.get("health_status")
    if (
        not isinstance(warning_count, int)
        or isinstance(warning_count, bool)
        or warning_count < 0
        or not isinstance(critical_count, int)
        or isinstance(critical_count, bool)
        or critical_count < 0
    ):
        return False
    expected = "critical" if critical_count else "warning" if warning_count else "healthy"
    return status == expected


def _print_sync_run_metrics(*, limit: int, duckdb_path: Path) -> None:
    from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

    print(f"== sync_run_metrics (most recent {limit} rows per scope) ==")
    conn = open_duckdb_connection(duckdb_path, read_only=True)
    try:
        for source, scope, schema in _OPS_TABLES:
            table = f'"{schema}"."sync_run_metrics"'
            try:
                rows = conn.execute(
                    f"""
                    select task_name, recorded_at, metrics_json
                    from {table}
                    order by recorded_at desc
                    limit ?
                    """,
                    [limit],
                ).fetchall()
            except Exception as exc:
                print(f"{source}/{scope}: unavailable ({exc.__class__.__name__})")
                continue
            if not rows:
                print(f"{source}/{scope}: (empty)")
                continue
            for task_name, recorded_at, metrics_json in rows:
                print(
                    _format_row(
                        source=source,
                        scope=scope,
                        task_name=str(task_name),
                        recorded_at=recorded_at,
                        metrics=_load_metrics(metrics_json),
                    )
                )
    finally:
        conn.close()


def _print_ingestion_run_events(*, limit: int, duckdb_path: Path) -> None:
    from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

    print(f"\n== ingestion_run_events (most recent {limit} rows per scope) ==")
    conn = open_duckdb_connection(duckdb_path, read_only=True)
    try:
        for source, scope, schema in _OPS_TABLES:
            table = f'"{schema}"."ingestion_run_events"'
            try:
                rows = conn.execute(
                    f"""
                    select task_name, recorded_at, metrics_json
                    from {table}
                    order by recorded_at desc
                    limit ?
                    """,
                    [limit],
                ).fetchall()
            except Exception as exc:
                print(f"{source}/{scope}: unavailable ({exc.__class__.__name__})")
                continue
            if not rows:
                print(f"{source}/{scope}: (empty)")
                continue
            for task_name, recorded_at, metrics_json in rows:
                print(
                    _format_row(
                        source=source,
                        scope=scope,
                        task_name=str(task_name),
                        recorded_at=recorded_at,
                        metrics=_load_metrics(metrics_json),
                    )
                )
    finally:
        conn.close()


def main() -> int:
    from oddsfox_pipeline.config import settings
    from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max rows per ops table (default: 10)",
    )
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="Warehouse path (default: active DUCKDB_PATH)",
    )
    parser.add_argument(
        "--scope",
        choices=("polymarket:soccer",),
        default=None,
        help="Evaluate one production-health contract instead of listing runs.",
    )
    parser.add_argument(
        "--fail-on",
        choices=("critical", "warning", "never"),
        default="never",
        help="Minimum health severity that returns exit 1 (default: never).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format for scoped health (default: text).",
    )
    args = parser.parse_args()
    duckdb_path = (args.duckdb_path or settings.DUCKDB_PATH).resolve()
    if args.duckdb_path is None:
        ensure_duck_db()
    limit = max(1, args.limit)
    if args.scope == "polymarket:soccer":
        from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

        conn = None
        try:
            conn = open_duckdb_connection(duckdb_path, read_only=True)
            rows = conn.execute(
                """
                select dagster_run_id, latest_run_status, latest_run_started_at,
                    latest_run_finished_at, warning_count, critical_count,
                    health_status, measured_at
                from polymarket_soccer_observability.polymarket_soccer_pipeline_health
                """
            ).fetchall()
        except Exception as exc:
            health = {
                "scope": args.scope,
                "status": "unavailable",
                "error": f"{exc.__class__.__name__}: {exc}",
            }
            print(
                json.dumps(health, default=str, sort_keys=True)
                if args.format == "json"
                else health
            )
            return 2
        finally:
            if conn is not None:
                conn.close()
        if len(rows) != 1:
            print(
                '{"status":"unavailable"}'
                if args.format == "json"
                else "health unavailable"
            )
            return 2
        keys = (
            "dagster_run_id",
            "latest_run_status",
            "latest_run_started_at",
            "latest_run_finished_at",
            "warning_count",
            "critical_count",
            "health_status",
            "measured_at",
        )
        health = dict(zip(keys, rows[0], strict=True))
        if not _valid_soccer_health(health):
            invalid = {"scope": args.scope, "status": "unavailable", "error": "invalid monitoring state"}
            print(
                json.dumps(invalid, sort_keys=True)
                if args.format == "json"
                else "health unavailable: invalid monitoring state"
            )
            return 2
        if args.format == "json":
            print(json.dumps(health, default=str, sort_keys=True))
        else:
            print(
                f"polymarket/soccer health={health['health_status']} "
                f"run={health['dagster_run_id']} status={health['latest_run_status']} "
                f"warnings={health['warning_count']} critical={health['critical_count']}"
            )
        if args.fail_on == "critical" and health["critical_count"]:
            return 1
        if args.fail_on == "warning" and (
            health["warning_count"] or health["critical_count"]
        ):
            return 1
        return 0
    _print_sync_run_metrics(limit=limit, duckdb_path=duckdb_path)
    _print_ingestion_run_events(limit=limit, duckdb_path=duckdb_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
