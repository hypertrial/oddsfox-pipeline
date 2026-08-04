import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import duckdb

from oddsfox_pipeline.config.settings_kalshi import (
    DEFAULT_KALSHI_WC2026_MARKET_SCOPE,
)
from oddsfox_pipeline.config.settings_polymarket import (
    DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
)
from oddsfox_pipeline.storage.duckdb.connection import (
    _use_conn,
    ensure_duck_db,
    get_connection,
    polymarket_wc2026_ops_tbl,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch import append_ingestion_run_event_stage
from oddsfox_pipeline.storage.duckdb.kalshi_dlt_batch import (
    append_kalshi_ingestion_run_event_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    kalshi_ops_tbl,
    polymarket_ops_tbl,
)

_BACKFILL_KEY_PREFIX = "backfill:"

logger = logging.getLogger(__name__)

OpsSource = str


def _default_scope(source: OpsSource) -> str:
    if source == "kalshi":
        return DEFAULT_KALSHI_WC2026_MARKET_SCOPE
    return DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE


def _ops_tbl(
    scope_name: str | None, table: str, *, source: OpsSource = "polymarket"
) -> str:
    scope = str(scope_name or _default_scope(source)).strip().lower()
    if source == "kalshi":
        return kalshi_ops_tbl(scope, table)
    return polymarket_ops_tbl(scope, table)


def _metadata_get(
    key: str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> Optional[str]:
    ensure_duck_db()
    with _use_conn(conn) as active:
        row = active.execute(
            f"SELECT value FROM {polymarket_wc2026_ops_tbl('scrape_metadata')} WHERE key = ?",
            [key],
        ).fetchone()
        return row[0] if row else None


def _metadata_set(
    key: str,
    value: str,
    conn: duckdb.DuckDBPyConnection | None = None,
):
    ensure_duck_db()
    with _use_conn(conn) as active:
        active.execute(
            f"""
            INSERT OR REPLACE INTO {polymarket_wc2026_ops_tbl("scrape_metadata")} (key, value)
            VALUES (?, ?)
            """,
            [key, value],
        )


def get_backfill_fully_checked(
    task: str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> Optional[bool]:
    """Return ledger status for a backfill task (True/False) or None if unset."""
    key = f"{_BACKFILL_KEY_PREFIX}{task}:fully_checked"
    raw = _metadata_get(key, conn)
    if raw is None:
        return None
    return raw.lower() in ("1", "true", "yes")


def set_backfill_fully_checked(
    task: str,
    fully_checked: bool,
    conn: duckdb.DuckDBPyConnection | None = None,
):
    """Persist ledger status for a backfill task and update timestamp."""
    now_iso = datetime.now(timezone.utc).isoformat()
    _metadata_set(
        f"{_BACKFILL_KEY_PREFIX}{task}:fully_checked",
        "1" if fully_checked else "0",
        conn,
    )
    _metadata_set(f"{_BACKFILL_KEY_PREFIX}{task}:timestamp", now_iso, conn)


def append_ingestion_run_event(
    task_name: str,
    metrics: dict[str, Any],
    *,
    recorded_at: Optional[datetime] = None,
    scope_name: str | None = None,
    source: OpsSource = "polymarket",
    conn: duckdb.DuckDBPyConnection | None = None,
) -> str:
    """
    Append one row to the append-only ingestion_run_events table for queryable audit history.

    Returns:
        run_id: UUID string primary key for this event.
    """
    ensure_duck_db()
    run_id = str(uuid.uuid4())
    ts = recorded_at if recorded_at is not None else datetime.now(timezone.utc)
    payload = dict(metrics)
    payload["timestamp"] = ts.isoformat()
    row = {
        "run_id": run_id,
        "task_name": task_name,
        "recorded_at": ts,
        "metrics_json": json.dumps(payload, sort_keys=True),
    }
    with _use_conn(conn) as active:
        if source == "kalshi":
            append_kalshi_ingestion_run_event_stage(
                row, active, scope_name=scope_name or _default_scope(source)
            )
        else:
            append_ingestion_run_event_stage(row, active, scope_name=scope_name)
    return run_id


def save_sync_run_metrics(
    task: str,
    metrics: dict[str, Any],
    history_limit: int = 20,
    *,
    scope_name: str | None = None,
    source: OpsSource = "polymarket",
    conn: duckdb.DuckDBPyConnection | None = None,
):
    """
    Persist latest sync metrics and a short rolling history in scrape_metadata.
    """
    recorded = datetime.now(timezone.utc)
    now_iso = recorded.isoformat()
    base_key = f"sync_metrics:{task}"
    payload = dict(metrics)
    payload["timestamp"] = now_iso

    try:
        append_ingestion_run_event(
            task,
            payload,
            recorded_at=recorded,
            scope_name=scope_name,
            source=source,
            conn=conn,
        )
    except Exception as exc:
        payload["ingestion_run_event_append_failed"] = True
        payload["ingestion_run_event_append_error"] = f"{exc.__class__.__name__}: {exc}"
        logger.warning(
            "ingestion_run_events append failed (continuing with scrape_metadata): %s",
            exc,
        )

    history: list[dict[str, Any]] = []
    if source == "polymarket":
        _metadata_set(f"{base_key}:last", json.dumps(payload, sort_keys=True), conn)

        history_key = f"{base_key}:history"
        history_raw = _metadata_get(history_key, conn)
        if history_raw:
            try:
                parsed = json.loads(history_raw)
                if isinstance(parsed, list):
                    history = [item for item in parsed if isinstance(item, dict)]
            except json.JSONDecodeError:
                history = []

        history.append(payload)
        if history_limit > 0:
            history = history[-int(history_limit) :]
        _metadata_set(history_key, json.dumps(history, sort_keys=True), conn)
    else:
        with _use_conn(conn) as active:
            try:
                row = active.execute(
                    f"""
                    SELECT history_json
                    FROM {_ops_tbl(scope_name, "sync_run_metrics", source=source)}
                    WHERE task_name = ?
                    """,
                    [task],
                ).fetchone()
            except Exception:
                row = None
        if row and row[0] is not None:
            try:
                parsed = json.loads(str(row[0]))
                if isinstance(parsed, list):
                    history = [item for item in parsed if isinstance(item, dict)]
            except json.JSONDecodeError:
                history = []
        history.append(payload)
        if history_limit > 0:
            history = history[-int(history_limit) :]
    with _use_conn(conn) as active:
        active.execute(
            f"""
            INSERT OR REPLACE INTO {_ops_tbl(scope_name, "sync_run_metrics", source=source)} (
                task_name, recorded_at, metrics_json, history_json
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                task,
                recorded,
                json.dumps(payload, sort_keys=True),
                json.dumps(history, sort_keys=True),
            ],
        )

    logger.info(
        "Persisted sync_metrics task=%s timestamp=%s keys=%s",
        task,
        now_iso,
        sorted(k for k in payload.keys() if k != "timestamp"),
    )


def get_sync_run_metrics(
    task: str,
    *,
    scope_name: str | None = None,
    source: OpsSource = "polymarket",
    conn: duckdb.DuckDBPyConnection | None = None,
) -> Optional[dict[str, Any]]:
    """Return the most recent sync metrics payload for task, if present."""
    ensure_duck_db()
    with _use_conn(conn) as active:
        try:
            row = active.execute(
                f"""
                SELECT metrics_json
                FROM {_ops_tbl(scope_name, "sync_run_metrics", source=source)}
                WHERE task_name = ?
                """,
                [task],
            ).fetchone()
        except Exception:
            row = None
    if row and row[0] is not None:
        try:
            parsed_table = json.loads(str(row[0]))
        except json.JSONDecodeError:
            parsed_table = None
        if isinstance(parsed_table, dict):
            return parsed_table

    raw = _metadata_get(f"sync_metrics:{task}:last", conn)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


POLYMARKET_TOKEN_HOURLY_ODDS_INCREMENTAL_MODEL = (
    "int_polymarket_wc2026_token_hourly_odds"
)
_DBT_INCREMENTAL_IN_PROGRESS_KEY = (
    f"dbt:incremental:{POLYMARKET_TOKEN_HOURLY_ODDS_INCREMENTAL_MODEL}:in_progress"
)


def mark_polymarket_token_hourly_odds_incremental_in_progress(
    conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    _metadata_set(_DBT_INCREMENTAL_IN_PROGRESS_KEY, "1", conn)


def clear_polymarket_token_hourly_odds_incremental_in_progress(
    conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    _metadata_set(_DBT_INCREMENTAL_IN_PROGRESS_KEY, "0", conn)


def polymarket_token_hourly_odds_incremental_recovery_needed(
    conn: duckdb.DuckDBPyConnection | None = None,
) -> bool:
    raw = _metadata_get(_DBT_INCREMENTAL_IN_PROGRESS_KEY, conn)
    return raw is not None and raw.lower() in ("1", "true", "yes")


_MARKET_SCOPE_DISCOVERY_PREFIX = "market_scope_discovery:"


def _scope_discovery_key(scope_name: str, suffix: str) -> str:
    scope = str(scope_name or "").strip().lower()
    if not scope:
        raise ValueError("scope_name must not be empty")
    return f"{_MARKET_SCOPE_DISCOVERY_PREFIX}{scope}:{suffix}"


def get_market_scope_discovery_fully_checked(
    scope_name: str = DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> Optional[bool]:
    """Return whether a full keyset market-scope discovery completed cleanly."""
    raw = _metadata_get(_scope_discovery_key(scope_name, "fully_checked"), conn)
    if raw is None:
        return None
    return raw.lower() in ("1", "true", "yes")


def get_market_scope_discovery_scope_config_hash(
    scope_name: str = DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> Optional[str]:
    raw = _metadata_get(_scope_discovery_key(scope_name, "scope_config_hash"), conn)
    return raw if raw else None


def set_market_scope_discovery_fully_checked(
    scope_name: str = DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
    fully_checked: bool = False,
    *,
    scope_config_hash: str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    """Persist full keyset discovery completion and scope config hash."""
    now_iso = datetime.now(timezone.utc).isoformat()
    _metadata_set(
        _scope_discovery_key(scope_name, "fully_checked"),
        "1" if fully_checked else "0",
        conn,
    )
    _metadata_set(
        _scope_discovery_key(scope_name, "scope_config_hash"),
        scope_config_hash,
        conn,
    )
    _metadata_set(_scope_discovery_key(scope_name, "last_run_at"), now_iso, conn)


def save_event_catalog_partition_checkpoint(
    conn: duckdb.DuckDBPyConnection,
    partition_key: str,
    stable_events: dict[str, dict[str, Any]],
    scan_summary: dict[str, Any],
    *,
    scope_name: str = DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
) -> None:
    """Persist one converged event-catalog partition for crash recovery."""
    key = str(partition_key or "").strip()
    if not key:
        raise ValueError("partition_key must not be empty")
    table = polymarket_ops_tbl(scope_name, "event_catalog_scan_checkpoint")
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {table}
            (partition_key, updated_at, stable_events_json, scan_summary_json)
        VALUES (?, ?, ?, ?)
        """,
        [
            key,
            datetime.now(timezone.utc),
            json.dumps(stable_events, separators=(",", ":"), default=str),
            json.dumps(scan_summary, separators=(",", ":"), default=str),
        ],
    )


def load_event_catalog_partition_checkpoints(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
) -> dict[str, dict[str, Any]]:
    """Load partition checkpoints as {partition_key: {stable_events, scan_summary}}."""
    table = polymarket_ops_tbl(scope_name, "event_catalog_scan_checkpoint")
    try:
        rows = conn.execute(
            f"""
            SELECT partition_key, stable_events_json, scan_summary_json
            FROM {table}
            """
        ).fetchall()
    except duckdb.CatalogException:
        return {}
    loaded: dict[str, dict[str, Any]] = {}
    for partition_key, events_json, summary_json in rows:
        try:
            stable_events = json.loads(events_json)
            scan_summary = json.loads(summary_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(stable_events, dict) or not isinstance(scan_summary, dict):
            continue
        loaded[str(partition_key)] = {
            "stable_events": stable_events,
            "scan_summary": scan_summary,
        }
    return loaded


def clear_event_catalog_partition_checkpoints(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE,
) -> None:
    """Drop all event-catalog partition checkpoints after a successful crawl."""
    table = polymarket_ops_tbl(scope_name, "event_catalog_scan_checkpoint")
    try:
        conn.execute(f"DELETE FROM {table}")
    except duckdb.CatalogException:
        return


__all__ = [
    "_metadata_get",
    "_metadata_set",
    "append_ingestion_run_event",
    "get_connection",
    "get_backfill_fully_checked",
    "set_backfill_fully_checked",
    "get_sync_run_metrics",
    "POLYMARKET_TOKEN_HOURLY_ODDS_INCREMENTAL_MODEL",
    "clear_polymarket_token_hourly_odds_incremental_in_progress",
    "mark_polymarket_token_hourly_odds_incremental_in_progress",
    "polymarket_token_hourly_odds_incremental_recovery_needed",
    "get_market_scope_discovery_fully_checked",
    "get_market_scope_discovery_scope_config_hash",
    "save_sync_run_metrics",
    "set_market_scope_discovery_fully_checked",
    "save_event_catalog_partition_checkpoint",
    "load_event_catalog_partition_checkpoints",
    "clear_event_catalog_partition_checkpoints",
]
