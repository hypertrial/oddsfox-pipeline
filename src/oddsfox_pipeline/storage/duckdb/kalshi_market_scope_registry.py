"""DuckDB persistence for Kalshi market-scope registry rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from oddsfox_pipeline.config.settings import DEFAULT_KALSHI_WC2026_MARKET_SCOPE
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.kalshi_dlt_batch import (
    load_kalshi_market_scope_registry_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import kalshi_ops_tbl


@dataclass(frozen=True)
class KalshiRegistryRow:
    market_ticker: str
    event_ticker: str
    series_ticker: str
    source: str
    scope_name: str = DEFAULT_KALSHI_WC2026_MARKET_SCOPE


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_scope(scope_name: str) -> str:
    normalized = str(scope_name or "").strip().lower()
    if not normalized:
        raise ValueError("scope_name must not be empty")
    return normalized


def upsert_registry_rows(rows: Sequence[KalshiRegistryRow]) -> int:
    if not rows:
        return 0
    ensure_duck_db()
    now = _utc_now()
    by_scope: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        scope = _normalize_scope(row.scope_name)
        by_scope.setdefault(scope, []).append(
            {
                "scope_name": scope,
                "market_ticker": row.market_ticker,
                "event_ticker": row.event_ticker,
                "series_ticker": row.series_ticker,
                "source": row.source,
                "refreshed_at": now,
            }
        )
    total = 0
    with get_connection() as conn:
        for scope, payload in by_scope.items():
            load_kalshi_market_scope_registry_stage(payload, conn, scope_name=scope)
            total += len(payload)
    return total


def get_registry_market_tickers(
    scope_name: str = DEFAULT_KALSHI_WC2026_MARKET_SCOPE,
) -> list[str]:
    ensure_duck_db()
    scope = _normalize_scope(scope_name)
    registry = kalshi_ops_tbl(scope, "market_scope_registry")
    with get_connection() as conn:
        result = conn.execute(
            f"""
            SELECT market_ticker
            FROM {registry}
            WHERE scope_name = ?
            ORDER BY market_ticker
            """,
            [scope],
        ).fetchall()
    return [str(row[0]) for row in result]


def registry_market_count(scope_name: str | None = None) -> int:
    ensure_duck_db()
    scope = _normalize_scope(scope_name or DEFAULT_KALSHI_WC2026_MARKET_SCOPE)
    registry = kalshi_ops_tbl(scope, "market_scope_registry")
    with get_connection() as conn:
        row = conn.execute(
            f"SELECT COUNT(*) FROM {registry} WHERE scope_name = ?",
            [scope],
        ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


__all__ = [
    "KalshiRegistryRow",
    "get_registry_market_tickers",
    "registry_market_count",
    "upsert_registry_rows",
]
