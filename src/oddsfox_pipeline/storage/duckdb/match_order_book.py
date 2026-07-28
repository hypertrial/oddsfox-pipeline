"""Transactional PMXT order-book scan ledger."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import uuid4

import duckdb

from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_wc2026_ops_tbl,
    polymarket_wc2026_raw_tbl,
)

RUNS = polymarket_wc2026_ops_tbl("match_order_book_scan_runs")
WINDOWS = polymarket_wc2026_ops_tbl("match_order_book_scan_windows")
SNAPSHOTS = polymarket_wc2026_raw_tbl("match_order_book_snapshots")
METADATA = polymarket_wc2026_ops_tbl("scrape_metadata")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _sanitize_error(exc: BaseException) -> str:
    # Exception text from HTTP clients may contain request headers or upstream
    # payloads. Persist only the local classification; Dagster receives the
    # structured, allowlisted scan summary instead.
    return f"{exc.__class__.__name__}: PMXT order-book scan failed"


def _require_lease(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    lease_owner: str,
    now: datetime,
) -> None:
    row = conn.execute(
        f"""
        SELECT status, lease_owner, lease_expires_at
        FROM {RUNS}
        WHERE scan_id = ?
        """,
        [scan_id],
    ).fetchone()
    if row is None:
        raise RuntimeError(f"PMXT order-book scan {scan_id} does not exist")
    expires = row[2]
    if row[0] != "running" or row[1] != lease_owner or not expires or expires <= now:
        raise RuntimeError(f"PMXT order-book scan {scan_id} lease was lost")


def acquire_scan(
    conn: duckdb.DuckDBPyConnection,
    *,
    manifest_version: int,
    manifest_sha256: str,
    targets: Iterable[Any],
    lease_owner: str,
    force: bool,
    lease_seconds: int = 300,
    now: datetime | None = None,
) -> tuple[str, bool, bool]:
    """Return ``(scan_id, published, resumed)`` and acquire its lease."""
    current = _utc_naive(now) if now else _utcnow()
    target_list = list(targets)
    token_count = sum(len(target.outcomes) for target in target_list)
    conn.execute("BEGIN TRANSACTION")
    try:
        published = conn.execute(
            f"""
            SELECT scan_id
            FROM {RUNS}
            WHERE manifest_sha256 = ? AND status = 'published' AND raw_published
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            [manifest_sha256],
        ).fetchone()
        if published and not force:
            conn.execute("COMMIT")
            return str(published[0]), True, False

        resumable = None
        if not force:
            resumable = conn.execute(
                f"""
                SELECT scan_id, lease_owner, lease_expires_at
                FROM {RUNS}
                WHERE manifest_sha256 = ?
                  AND status IN ('running', 'paused', 'failed')
                ORDER BY started_at DESC
                LIMIT 1
                """,
                [manifest_sha256],
            ).fetchone()
        if resumable:
            scan_id, active_owner, expires_at = resumable
            expires = expires_at
            if (
                active_owner
                and active_owner != lease_owner
                and expires is not None
                and expires > current
            ):
                raise RuntimeError(
                    f"PMXT order-book scan {scan_id} is leased by another run"
                )
            conn.execute(
                f"""
                UPDATE {RUNS}
                SET status = 'running',
                    lease_owner = ?,
                    lease_expires_at = ?,
                    last_checkpoint_at = ?,
                    error_type = NULL,
                    error_message = NULL
                WHERE scan_id = ?
                """,
                [
                    lease_owner,
                    current + timedelta(seconds=lease_seconds),
                    current,
                    scan_id,
                ],
            )
            conn.execute("COMMIT")
            return str(scan_id), False, True

        scan_id = str(uuid4())
        conn.execute(
            f"""
            INSERT INTO {RUNS} (
                scan_id, manifest_version, manifest_sha256, target_count,
                token_count, status, raw_published, lease_owner,
                lease_expires_at, started_at, last_checkpoint_at
            )
            VALUES (?, ?, ?, ?, ?, 'running', FALSE, ?, ?, ?, ?)
            """,
            [
                scan_id,
                manifest_version,
                manifest_sha256,
                len(target_list),
                token_count,
                lease_owner,
                current + timedelta(seconds=lease_seconds),
                current,
                current,
            ],
        )
        for target in target_list:
            for outcome in target.outcomes:
                conn.execute(
                    f"""
                    INSERT INTO {WINDOWS} (
                        scan_id, fifa_match_id, market_id, condition_id,
                        outcome_label, clob_token_id, window_start_ms,
                        window_end_ms, depth, status, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'pending', ?)
                    """,
                    [
                        scan_id,
                        target.fifa_match_id,
                        target.market_id,
                        target.condition_id,
                        outcome.label,
                        outcome.clob_token_id,
                        target.window_start_ms,
                        target.window_end_ms,
                        current,
                    ],
                )
        conn.execute("COMMIT")
        return scan_id, False, False
    except Exception:
        conn.execute("ROLLBACK")
        raise


def next_pending_window(
    conn: duckdb.DuckDBPyConnection, scan_id: str
) -> dict[str, Any] | None:
    cursor = conn.execute(
        f"""
        SELECT fifa_match_id, market_id, condition_id, outcome_label,
               clob_token_id, window_start_ms, window_end_ms, depth
        FROM {WINDOWS}
        WHERE scan_id = ? AND status = 'pending'
        ORDER BY depth, clob_token_id, window_start_ms
        LIMIT 1
        """,
        [scan_id],
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(zip([item[0] for item in cursor.description], row, strict=True))


def reserve_api_attempt(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    lease_owner: str,
    token_id: str,
    window_start_ms: int,
    window_end_ms: int,
    monthly_credit_budget: int,
    now: datetime | None = None,
) -> bool:
    """Reserve one PMXT request before issuing it, enforcing a local UTC cap."""
    current = _utc_naive(now) if now else _utcnow()
    key = f"pmxt_order_book_api_attempts_{date(current.year, current.month, 1)}"
    conn.execute("BEGIN TRANSACTION")
    try:
        _require_lease(
            conn,
            scan_id=scan_id,
            lease_owner=lease_owner,
            now=current,
        )
        current_usage_row = conn.execute(
            f"SELECT value FROM {METADATA} WHERE key = ?", [key]
        ).fetchone()
        current_usage = int(current_usage_row[0]) if current_usage_row else 0
        if current_usage >= monthly_credit_budget:
            conn.execute("COMMIT")
            return False
        conn.execute(
            f"""
            INSERT INTO {METADATA} (key, value)
            VALUES (?, '1')
            ON CONFLICT (key) DO UPDATE
            SET value = CAST(CAST({METADATA}.value AS BIGINT) + 1 AS VARCHAR)
            """,
            [key],
        )
        updated = conn.execute(
            f"""
            UPDATE {WINDOWS}
            SET api_attempt_count = api_attempt_count + 1, updated_at = ?
            WHERE scan_id = ? AND clob_token_id = ?
              AND window_start_ms = ? AND window_end_ms = ?
            """,
            [current, scan_id, token_id, window_start_ms, window_end_ms],
        ).fetchone()[0]
        if int(updated) != 1:
            raise RuntimeError("PMXT work window disappeared before request")
        conn.execute(
            f"""
            UPDATE {RUNS}
            SET api_attempt_count = api_attempt_count + 1,
                last_checkpoint_at = ?,
                lease_expires_at = ? + INTERVAL 15 MINUTE
            WHERE scan_id = ? AND lease_owner = ? AND status = 'running'
            """,
            [current, current, scan_id, lease_owner],
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def split_window(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    lease_owner: str,
    window: dict[str, Any],
) -> None:
    start = int(window["window_start_ms"])
    end = int(window["window_end_ms"])
    if end - start <= 1:
        raise RuntimeError(
            f"PMXT returned 1000 snapshots for irreducible range {start}..{end}"
        )
    midpoint = (start + end) // 2
    children = ((start, midpoint), (midpoint, end))
    now = _utcnow()
    conn.execute("BEGIN TRANSACTION")
    try:
        _require_lease(
            conn,
            scan_id=scan_id,
            lease_owner=lease_owner,
            now=now,
        )
        conn.execute(
            f"""
            UPDATE {WINDOWS}
            SET status = 'split', updated_at = ?
            WHERE scan_id = ? AND clob_token_id = ?
              AND window_start_ms = ? AND window_end_ms = ?
            """,
            [now, scan_id, window["clob_token_id"], start, end],
        )
        for child_start, child_end in children:
            conn.execute(
                f"""
                INSERT INTO {WINDOWS} (
                    scan_id, fifa_match_id, market_id, condition_id,
                    outcome_label, clob_token_id, window_start_ms,
                    window_end_ms, depth, status, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT DO NOTHING
                """,
                [
                    scan_id,
                    window["fifa_match_id"],
                    window["market_id"],
                    window["condition_id"],
                    window["outcome_label"],
                    window["clob_token_id"],
                    child_start,
                    child_end,
                    int(window["depth"]) + 1,
                    now,
                ],
            )
        conn.execute(
            f"""
            UPDATE {RUNS}
            SET last_checkpoint_at = ?,
                lease_expires_at = ? + INTERVAL 5 MINUTE
            WHERE scan_id = ? AND lease_owner = ? AND status = 'running'
            """,
            [now, now, scan_id, lease_owner],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def complete_window(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    lease_owner: str,
    window: dict[str, Any],
    snapshot_hashes: list[str],
) -> None:
    unique_hashes = sorted(set(snapshot_hashes))
    if len(unique_hashes) != len(snapshot_hashes) or any(
        not _SHA256_RE.fullmatch(value) for value in unique_hashes
    ):
        raise ValueError("PMXT window snapshot hashes must be unique SHA-256 values")
    content_sha256 = (
        hashlib.sha256("".join(unique_hashes).encode()).hexdigest()
        if unique_hashes
        else None
    )
    snapshot_hashes_json = json.dumps(unique_hashes, separators=(",", ":"))
    status = "loaded" if unique_hashes else "empty"
    now = _utcnow()
    conn.execute("BEGIN TRANSACTION")
    try:
        _require_lease(
            conn,
            scan_id=scan_id,
            lease_owner=lease_owner,
            now=now,
        )
        conn.execute(
            f"""
            UPDATE {WINDOWS}
            SET status = ?, snapshot_count = ?, content_sha256 = ?,
                snapshot_hashes_json = ?,
                updated_at = ?, error_type = NULL, error_message = NULL
            WHERE scan_id = ? AND clob_token_id = ?
              AND window_start_ms = ? AND window_end_ms = ?
            """,
            [
                status,
                len(unique_hashes),
                content_sha256,
                snapshot_hashes_json,
                now,
                scan_id,
                window["clob_token_id"],
                window["window_start_ms"],
                window["window_end_ms"],
            ],
        )
        conn.execute(
            f"""
            UPDATE {RUNS}
            SET last_checkpoint_at = ?,
                lease_expires_at = ? + INTERVAL 5 MINUTE
            WHERE scan_id = ? AND lease_owner = ? AND status = 'running'
            """,
            [now, now, scan_id, lease_owner],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def set_scan_status(
    conn: duckdb.DuckDBPyConnection,
    scan_id: str,
    status: str,
    exc: BaseException | None = None,
    *,
    lease_owner: str,
) -> None:
    now = _utcnow()
    conn.execute(
        f"""
        UPDATE {RUNS}
        SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
            last_checkpoint_at = ?,
            finished_at = CASE WHEN ? = 'published' THEN ? ELSE finished_at END,
            error_type = ?, error_message = ?
        WHERE scan_id = ? AND lease_owner = ?
        """,
        [
            status,
            now,
            status,
            now,
            exc.__class__.__name__ if exc else None,
            _sanitize_error(exc) if exc else None,
            scan_id,
            lease_owner,
        ],
    )


def publish_scan(
    conn: duckdb.DuckDBPyConnection,
    scan_id: str,
    *,
    lease_owner: str,
) -> dict[str, Any]:
    """Validate the complete work tree and atomically expose this scan."""
    conn.execute("BEGIN TRANSACTION")
    try:
        current = _utcnow()
        _require_lease(
            conn,
            scan_id=scan_id,
            lease_owner=lease_owner,
            now=current,
        )
        incomplete = int(
            conn.execute(
                f"""
                SELECT count(*)
                FROM {WINDOWS}
                WHERE scan_id = ? AND status IN ('pending', 'failed')
                """,
                [scan_id],
            ).fetchone()[0]
        )
        if incomplete:
            raise RuntimeError(f"PMXT scan has {incomplete} incomplete windows")
        invalid_windows = int(
            conn.execute(
                f"""
                SELECT count(*)
                FROM {WINDOWS}
                WHERE scan_id = ?
                  AND (
                    (status = 'loaded' AND (
                        snapshot_count <= 0
                        OR content_sha256 IS NULL
                        OR json_array_length(snapshot_hashes_json)
                           != snapshot_count
                    ))
                    OR (status = 'empty' AND (
                        snapshot_count != 0
                        OR content_sha256 IS NOT NULL
                        OR snapshot_hashes_json != '[]'
                    ))
                    OR (status = 'split' AND (
                        snapshot_count != 0
                        OR content_sha256 IS NOT NULL
                        OR snapshot_hashes_json != '[]'
                    ))
                  )
                """,
                [scan_id],
            ).fetchone()[0]
        )
        if invalid_windows:
            raise RuntimeError(
                f"PMXT scan has {invalid_windows} invalid window checkpoints"
            )
        expected_tokens = int(
            conn.execute(
                f"SELECT token_count FROM {RUNS} WHERE scan_id = ?", [scan_id]
            ).fetchone()[0]
        )
        window_hash_rows = conn.execute(
            f"""
            SELECT clob_token_id, snapshot_hashes_json
            FROM {WINDOWS}
            WHERE scan_id = ? AND status = 'loaded'
            """,
            [scan_id],
        ).fetchall()
        expected_snapshot_keys: set[tuple[str, str]] = set()
        for token_id, raw_hashes in window_hash_rows:
            hashes = json.loads(str(raw_hashes))
            if not isinstance(hashes, list) or any(
                not isinstance(value, str) or not _SHA256_RE.fullmatch(value)
                for value in hashes
            ):
                raise RuntimeError("PMXT window hash inventory is malformed")
            expected_snapshot_keys.update(
                (str(token_id), snapshot_hash) for snapshot_hash in hashes
            )
        inventory = conn.execute(
            f"""
            SELECT count(*), count(DISTINCT clob_token_id)
            FROM {SNAPSHOTS}
            WHERE scan_id = ?
            """,
            [scan_id],
        ).fetchone()
        snapshot_count, observed_tokens = map(int, inventory)
        if snapshot_count <= 0 or observed_tokens != expected_tokens:
            raise RuntimeError(
                "PMXT scan snapshot inventory is incomplete: "
                f"{snapshot_count} snapshots across {observed_tokens}/"
                f"{expected_tokens} tokens"
            )
        raw_snapshot_keys = {
            (str(row[0]), str(row[1]))
            for row in conn.execute(
                f"""
                SELECT clob_token_id, snapshot_sha256
                FROM {SNAPSHOTS}
                WHERE scan_id = ?
                """,
                [scan_id],
            ).fetchall()
        }
        if (
            len(raw_snapshot_keys) != snapshot_count
            or raw_snapshot_keys != expected_snapshot_keys
        ):
            raise RuntimeError(
                "PMXT raw snapshot inventory does not match completed window hashes"
            )
        hashes = sorted(snapshot_hash for _, snapshot_hash in raw_snapshot_keys)
        aggregate = hashlib.sha256("".join(hashes).encode()).hexdigest()
        now = _utcnow()
        conn.execute(
            f"""
            UPDATE {RUNS}
            SET status = 'published', raw_published = TRUE,
                snapshot_count = ?, aggregate_sha256 = ?,
                lease_owner = NULL, lease_expires_at = NULL,
                last_checkpoint_at = ?, finished_at = ?,
                error_type = NULL, error_message = NULL
            WHERE scan_id = ? AND lease_owner = ? AND status = 'running'
            """,
            [snapshot_count, aggregate, now, now, scan_id, lease_owner],
        )
        attempts = int(
            conn.execute(
                f"SELECT api_attempt_count FROM {RUNS} WHERE scan_id = ?", [scan_id]
            ).fetchone()[0]
        )
        conn.execute("COMMIT")
        return {
            "status": "published",
            "scan_id": scan_id,
            "snapshot_count": snapshot_count,
            "token_count": observed_tokens,
            "api_attempt_count": attempts,
            "aggregate_sha256": aggregate,
        }
    except Exception:
        conn.execute("ROLLBACK")
        raise


def scan_progress_summary(
    conn: duckdb.DuckDBPyConnection, scan_id: str
) -> dict[str, Any]:
    """Return allowlisted scan diagnostics suitable for Dagster metadata."""
    run = conn.execute(
        f"""
        SELECT
            manifest_sha256, target_count, token_count, status,
            api_attempt_count, snapshot_count, aggregate_sha256
        FROM {RUNS}
        WHERE scan_id = ?
        """,
        [scan_id],
    ).fetchone()
    if run is None:
        raise RuntimeError(f"PMXT order-book scan {scan_id} not found")
    windows = conn.execute(
        f"""
        SELECT
            count(*) FILTER (WHERE status IN ('pending', 'failed')),
            count(*) FILTER (WHERE status IN ('loaded', 'empty')),
            count(*) FILTER (WHERE status = 'split')
        FROM {WINDOWS}
        WHERE scan_id = ?
        """,
        [scan_id],
    ).fetchone()
    snapshots = conn.execute(
        f"""
        SELECT
            count(*),
            count(DISTINCT clob_token_id),
            min(snapshot_timestamp_ms),
            max(snapshot_timestamp_ms),
            coalesce(sum(
                json_array_length(bids_json) + json_array_length(asks_json)
            ), 0),
            count(*) FILTER (
                WHERE json_array_length(bids_json) = 0
                  AND json_array_length(asks_json) = 0
            )
        FROM {SNAPSHOTS}
        WHERE scan_id = ?
        """,
        [scan_id],
    ).fetchone()
    return {
        "status": str(run[3]),
        "scan_id": scan_id,
        "manifest_sha256": str(run[0]),
        "target_count": int(run[1]),
        "token_count": int(run[2]),
        "api_attempt_count": int(run[4]),
        "snapshot_count": int(snapshots[0]),
        "observed_token_count": int(snapshots[1]),
        "remaining_window_count": int(windows[0]),
        "completed_window_count": int(windows[1]),
        "split_window_count": int(windows[2]),
        "level_count": int(snapshots[4]),
        "empty_book_warning_count": int(snapshots[5]),
        "min_snapshot_timestamp_ms": (
            int(snapshots[2]) if snapshots[2] is not None else None
        ),
        "max_snapshot_timestamp_ms": (
            int(snapshots[3]) if snapshots[3] is not None else None
        ),
        "aggregate_sha256": str(run[6]) if run[6] is not None else None,
    }


def published_scan_summary(
    conn: duckdb.DuckDBPyConnection, scan_id: str
) -> dict[str, Any]:
    exists = conn.execute(
        f"""
        SELECT 1
        FROM {RUNS}
        WHERE scan_id = ? AND status = 'published' AND raw_published
        """,
        [scan_id],
    ).fetchone()
    if exists is None:
        raise RuntimeError(f"Published PMXT scan {scan_id} not found")
    return {**scan_progress_summary(conn, scan_id), "noop": True}


__all__ = [
    "acquire_scan",
    "complete_window",
    "next_pending_window",
    "publish_scan",
    "published_scan_summary",
    "reserve_api_attempt",
    "scan_progress_summary",
    "set_scan_status",
    "split_window",
]
