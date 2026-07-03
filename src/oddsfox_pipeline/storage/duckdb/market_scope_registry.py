"""DuckDB persistence for Polymarket market-scope registry rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Sequence

from oddsfox_pipeline.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.dlt_batch import load_market_scope_registry_stage
from oddsfox_pipeline.storage.duckdb.schemas.constants import wc2026_polymarket_ops_tbl

_TAB_REGISTRY = wc2026_polymarket_ops_tbl("market_scope_registry")


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
    payload = [
        {
            "scope_name": _normalize_scope(r.scope_name),
            "market_id": r.market_id,
            "event_slug": r.event_slug,
            "event_id": r.event_id,
            "source": r.source,
            "refreshed_at": now,
        }
        for r in rows
    ]
    with get_connection() as conn:
        load_market_scope_registry_stage(payload, conn)
    return len(payload)


def get_registry_market_ids(scope_name: str = DEFAULT_MARKET_SCOPE) -> List[str]:
    ensure_duck_db()
    scope = _normalize_scope(scope_name)
    with get_connection() as conn:
        result = conn.execute(
            f"""
            SELECT market_id
            FROM {_TAB_REGISTRY}
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
            row = conn.execute(
                f"SELECT COUNT(*) FROM {_TAB_REGISTRY} WHERE scope_name = ?",
                [_normalize_scope(scope_name)],
            ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def clear_registry(scope_name: str | None = None) -> None:
    """Remove registry rows; optionally limit to one scope (tests only)."""
    ensure_duck_db()
    with get_connection() as conn:
        if scope_name is None:
            conn.execute(f"DELETE FROM {_TAB_REGISTRY}")
        else:
            conn.execute(
                f"DELETE FROM {_TAB_REGISTRY} WHERE scope_name = ?",
                [_normalize_scope(scope_name)],
            )


def get_registry_event_slugs(scope_name: str | None = None) -> List[str]:
    """Return distinct non-null event_slug values from the scope registry."""
    ensure_duck_db()
    params: list[str] = []
    where_scope = ""
    if scope_name is not None:
        where_scope = "AND scope_name = ?"
        params.append(_normalize_scope(scope_name))
    with get_connection() as conn:
        result = conn.execute(
            f"""
            SELECT DISTINCT event_slug
            FROM {_TAB_REGISTRY}
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
