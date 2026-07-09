"""Kalshi candlestick persistence and ledger helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from oddsfox_pipeline.config.settings import DEFAULT_KALSHI_WC2026_MARKET_SCOPE
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.kalshi_dlt_batch import (
    load_kalshi_candlesticks_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    kalshi_ops_tbl,
    kalshi_raw_tbl,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_registry_markets_for_sync(
    *,
    scope_name: str = DEFAULT_KALSHI_WC2026_MARKET_SCOPE,
    force: bool = False,
) -> list[dict[str, Any]]:
    ensure_duck_db()
    scope = str(scope_name).strip().lower()
    registry = kalshi_ops_tbl(scope, "market_scope_registry")
    markets = kalshi_raw_tbl(scope, "markets")
    ledger = kalshi_ops_tbl(scope, "candlestick_sync_ledger")
    due_filter = ""
    if not force:
        due_filter = """
        AND (
            l.market_ticker IS NULL
            OR l.next_check_at IS NULL
            OR l.next_check_at <= CURRENT_TIMESTAMP
        )
        """
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                r.market_ticker,
                r.series_ticker,
                m.open_time
            FROM {registry} r
            LEFT JOIN {markets} m ON m.market_ticker = r.market_ticker
            LEFT JOIN {ledger} l ON l.market_ticker = r.market_ticker
            WHERE r.scope_name = ?
            {due_filter}
            ORDER BY r.market_ticker
            """,
            [scope],
        ).fetchall()
    return [
        {
            "market_ticker": str(row[0]),
            "series_ticker": str(row[1]),
            "open_time": row[2],
        }
        for row in rows
    ]


def save_candlesticks_batch(rows: Sequence[dict[str, Any]]) -> int:
    if not rows:
        return 0
    ensure_duck_db()
    with get_connection() as conn:
        load_kalshi_candlesticks_stage(rows, conn)
    return len(rows)


def upsert_candlestick_ledger_state(
    *,
    market_ticker: str,
    fully_checked: bool,
    empty_run: bool,
    routine_interval_hours: int = 1,
) -> None:
    ensure_duck_db()
    now = _utc_now()
    next_check = now + timedelta(hours=routine_interval_hours)
    ledger = kalshi_ops_tbl(
        DEFAULT_KALSHI_WC2026_MARKET_SCOPE, "candlestick_sync_ledger"
    )
    with get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO {ledger} (
                market_ticker,
                last_sync_hour_start,
                fully_checked,
                last_checked_at,
                next_check_at,
                empty_run_streak
            )
            VALUES (?, NULL, ?, ?, ?, ?)
            ON CONFLICT(market_ticker) DO UPDATE SET
                fully_checked=excluded.fully_checked,
                last_checked_at=excluded.last_checked_at,
                next_check_at=excluded.next_check_at,
                empty_run_streak=CASE
                    WHEN ? THEN COALESCE({ledger}.empty_run_streak, 0) + 1
                    ELSE 0
                END
            """,
            [
                market_ticker,
                fully_checked,
                now.replace(tzinfo=None),
                next_check.replace(tzinfo=None),
                1 if empty_run else 0,
                empty_run,
            ],
        )


__all__ = [
    "get_registry_markets_for_sync",
    "save_candlesticks_batch",
    "upsert_candlestick_ledger_state",
]
