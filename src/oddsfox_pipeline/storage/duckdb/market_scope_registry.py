"""DuckDB persistence for Polymarket market-scope registry rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Sequence

from oddsfox_pipeline.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.dlt_batch import load_market_scope_registry_stage
from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_ops_tbl

_TAB_REGISTRY = polymarket_ops_tbl(DEFAULT_MARKET_SCOPE, "market_scope_registry")


def _registry_tbl(scope_name: str) -> str:
    return polymarket_ops_tbl(_normalize_scope(scope_name), "market_scope_registry")


@dataclass(frozen=True)
class RegistryRow:
    market_id: str
    event_slug: str | None
    event_id: str | None
    source: str
    scope_name: str = DEFAULT_MARKET_SCOPE


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_scope(scope_name: str) -> str:
    normalized = str(scope_name or "").strip().lower()
    if not normalized:
        raise ValueError("scope_name must not be empty")
    return normalized


def upsert_registry_rows(rows: Sequence[RegistryRow]) -> int:
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
                "market_id": row.market_id,
                "event_slug": row.event_slug,
                "event_id": row.event_id,
                "source": row.source,
                "refreshed_at": now,
            }
        )
    total = 0
    with get_connection() as conn:
        for scope, payload in by_scope.items():
            load_market_scope_registry_stage(payload, conn, scope_name=scope)
            total += len(payload)
    return total


def get_registry_market_ids(scope_name: str = DEFAULT_MARKET_SCOPE) -> List[str]:
    ensure_duck_db()
    scope = _normalize_scope(scope_name)
    registry = _registry_tbl(scope)
    with get_connection() as conn:
        result = conn.execute(
            f"""
            SELECT market_id
            FROM {registry}
            WHERE scope_name = ?
            ORDER BY market_id
            """,
            [scope],
        ).fetchall()
    return [str(row[0]) for row in result]


def registry_market_count(scope_name: str | None = None) -> int:
    ensure_duck_db()
    with get_connection() as conn:
        if scope_name is None:
            row = conn.execute(f"SELECT COUNT(*) FROM {_TAB_REGISTRY}").fetchone()
        else:
            registry = _registry_tbl(scope_name)
            row = conn.execute(
                f"SELECT COUNT(*) FROM {registry} WHERE scope_name = ?",
                [_normalize_scope(scope_name)],
            ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def clear_registry(scope_name: str | None = None) -> None:
    """Remove registry rows; optionally limit to one scope (tests only)."""
    ensure_duck_db()
    scope = _normalize_scope(scope_name) if scope_name is not None else None
    with get_connection() as conn:
        if scope is None:
            conn.execute(f"DELETE FROM {_TAB_REGISTRY}")
        else:
            conn.execute(
                f"DELETE FROM {_registry_tbl(scope)} WHERE scope_name = ?",
                [scope],
            )


def get_registry_event_slugs(scope_name: str | None = None) -> List[str]:
    """Return distinct non-null event_slug values from the scope registry."""
    ensure_duck_db()
    params: list[str] = []
    where_scope = ""
    registry = _TAB_REGISTRY
    if scope_name is not None:
        scope = _normalize_scope(scope_name)
        registry = _registry_tbl(scope)
        where_scope = "AND scope_name = ?"
        params.append(scope)
    with get_connection() as conn:
        result = conn.execute(
            f"""
            SELECT DISTINCT event_slug
            FROM {registry}
            WHERE event_slug IS NOT NULL
              AND TRIM(event_slug) != ''
              {where_scope}
            ORDER BY event_slug
            """,
            params,
        ).fetchall()
    return [str(row[0]) for row in result]


__all__ = [
    "RegistryRow",
    "clear_registry",
    "get_registry_event_slugs",
    "get_registry_market_ids",
    "registry_market_count",
    "upsert_registry_rows",
]
