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
    return (
        f"{recorded_at}  {source}/{scope}  {task_name:<28}  "
        f"{status:<8}{suffix}"
    )


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
    args = parser.parse_args()
    duckdb_path = (args.duckdb_path or settings.DUCKDB_PATH).resolve()
    ensure_duck_db()
    limit = max(1, args.limit)
    _print_sync_run_metrics(limit=limit, duckdb_path=duckdb_path)
    _print_ingestion_run_events(limit=limit, duckdb_path=duckdb_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
