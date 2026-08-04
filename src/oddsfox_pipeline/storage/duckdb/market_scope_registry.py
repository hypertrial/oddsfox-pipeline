"""DuckDB persistence for Polymarket market-scope registry rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Sequence

from oddsfox_pipeline.config.settings_polymarket import (
    POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
)
from oddsfox_pipeline.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.dlt_batch import load_market_scope_registry_stage
from oddsfox_pipeline.storage.duckdb.registry_common import _normalize_scope, _utc_now
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

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
    event_volume_usd_lifetime_reported: float | None = None
    is_event_volume_eligible: bool | None = None
    first_eligible_at: datetime | None = None


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
                "event_volume_usd_lifetime_reported": (
                    row.event_volume_usd_lifetime_reported
                ),
                "is_event_volume_eligible": row.is_event_volume_eligible,
                "first_eligible_at": row.first_eligible_at,
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


def _load_existing_event_eligibility(
    conn: Any,
    *,
    scope_name: str,
) -> dict[str, tuple[bool, datetime | None]]:
    registry = _registry_tbl(scope_name)
    rows = conn.execute(
        f"""
        SELECT
            event_id,
            bool_or(coalesce(is_event_volume_eligible, false)) AS was_eligible,
            min(first_eligible_at) AS first_eligible_at
        FROM {registry}
        WHERE scope_name = ?
          AND event_id IS NOT NULL
          AND trim(event_id) != ''
        GROUP BY event_id
        """,
        [scope_name],
    ).fetchall()
    return {
        str(event_id): (bool(was_eligible), first_eligible_at)
        for event_id, was_eligible, first_eligible_at in rows
    }


def _load_latest_event_snapshots(
    conn: Any,
    *,
    scope_name: str,
) -> list[dict[str, Any]]:
    event_snapshots = polymarket_raw_tbl(scope_name, "event_snapshots")
    rows = conn.execute(
        f"""
        SELECT
            event_id,
            event_slug,
            event_volume_usd_lifetime_reported,
            observed_at
        FROM {event_snapshots}
        QUALIFY row_number() OVER (
            PARTITION BY event_id
            ORDER BY observed_at DESC
        ) = 1
        """
    ).fetchall()
    return [
        {
            "event_id": str(event_id),
            "event_slug": str(event_slug) if event_slug is not None else None,
            "event_volume_usd_lifetime_reported": volume,
            "observed_at": observed_at,
        }
        for event_id, event_slug, volume, observed_at in rows
    ]


def _load_enclosing_market_memberships(
    conn: Any,
    *,
    scope_name: str,
    eligible_event_ids: set[str],
) -> list[dict[str, str]]:
    if not eligible_event_ids:
        return []
    event_market_snapshots = polymarket_raw_tbl(scope_name, "event_market_snapshots")
    placeholders = ", ".join("?" for _ in eligible_event_ids)
    rows = conn.execute(
        f"""
        SELECT event_id, market_id
        FROM {event_market_snapshots}
        WHERE is_enclosing_event
          AND event_id IN ({placeholders})
        QUALIFY row_number() OVER (
            PARTITION BY market_id
            ORDER BY observed_at DESC
        ) = 1
        """,
        list(eligible_event_ids),
    ).fetchall()
    return [
        {"event_id": str(event_id), "market_id": str(market_id)}
        for event_id, market_id in rows
    ]


def build_registry_rows_from_event_catalog(
    *,
    scope_name: str = DEFAULT_MARKET_SCOPE,
    event_min_volume_usd: float = POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
    seed_rows: Sequence[RegistryRow] = (),
) -> list[RegistryRow]:
    """Admit all enclosing-event markets for sticky event-volume-eligible events."""
    ensure_duck_db()
    scope = _normalize_scope(scope_name)
    now = _utc_now()
    with get_connection() as conn:
        existing = _load_existing_event_eligibility(conn, scope_name=scope)
        latest_events = _load_latest_event_snapshots(conn, scope_name=scope)

    eligible_events: dict[str, dict[str, Any]] = {}
    for event in latest_events:
        event_id = event["event_id"]
        volume = event.get("event_volume_usd_lifetime_reported")
        was_eligible, first_at = existing.get(event_id, (False, None))
        meets_floor = volume is not None and float(volume) >= event_min_volume_usd
        is_eligible = was_eligible or meets_floor
        if not is_eligible:
            continue
        first_eligible_at = first_at if was_eligible else (first_at or now)
        eligible_events[event_id] = {
            **event,
            "is_event_volume_eligible": True,
            "first_eligible_at": first_eligible_at,
        }

    with get_connection() as conn:
        memberships = _load_enclosing_market_memberships(
            conn,
            scope_name=scope,
            eligible_event_ids=set(eligible_events),
        )

    rows: list[RegistryRow] = []
    for membership in memberships:
        event = eligible_events[membership["event_id"]]
        rows.append(
            RegistryRow(
                scope_name=scope,
                market_id=membership["market_id"],
                event_slug=event.get("event_slug"),
                event_id=membership["event_id"],
                source="event_catalog",
                event_volume_usd_lifetime_reported=event.get(
                    "event_volume_usd_lifetime_reported"
                ),
                is_event_volume_eligible=True,
                first_eligible_at=event.get("first_eligible_at"),
            )
        )
    rows.extend(seed_rows)
    return rows


__all__ = [
    "RegistryRow",
    "build_registry_rows_from_event_catalog",
    "clear_registry",
    "get_registry_event_slugs",
    "get_registry_market_ids",
    "registry_market_count",
    "upsert_registry_rows",
]
