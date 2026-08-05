"""DuckDB persistence for Polymarket market-scope registry rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Sequence

from oddsfox_pipeline.config.settings_polymarket import (
    POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
)
from oddsfox_pipeline.ingestion.polymarket.polymarket_ids import (
    is_numeric_polymarket_id,
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


def prune_stale_event_catalog_registry_rows(
    *,
    scope_name: str,
    active_market_ids: Sequence[str],
) -> int:
    """Drop event-catalog registry rows that are no longer admitted."""
    ensure_duck_db()
    scope = _normalize_scope(scope_name)
    registry = _registry_tbl(scope)
    active_ids = [str(market_id) for market_id in active_market_ids]
    with get_connection() as conn:
        if not active_ids:
            count_row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {registry}
                WHERE scope_name = ?
                  AND source = 'event_catalog'
                """,
                [scope],
            ).fetchone()
            conn.execute(
                f"""
                DELETE FROM {registry}
                WHERE scope_name = ?
                  AND source = 'event_catalog'
                """,
                [scope],
            )
        else:
            placeholders = ", ".join("?" for _ in active_ids)
            count_row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {registry}
                WHERE scope_name = ?
                  AND source = 'event_catalog'
                  AND market_id NOT IN ({placeholders})
                """,
                [scope, *active_ids],
            ).fetchone()
            conn.execute(
                f"""
                DELETE FROM {registry}
                WHERE scope_name = ?
                  AND source = 'event_catalog'
                  AND market_id NOT IN ({placeholders})
                """,
                [scope, *active_ids],
            )
    return int(count_row[0]) if count_row and count_row[0] is not None else 0


def prune_ineligible_api_registry_rows(*, scope_name: str) -> int:
    """Drop events_api/markets_api rows that are not volume-eligible."""
    ensure_duck_db()
    scope = _normalize_scope(scope_name)
    registry = _registry_tbl(scope)
    with get_connection() as conn:
        count_row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {registry}
            WHERE scope_name = ?
              AND source IN ('events_api', 'markets_api')
              AND NOT coalesce(is_event_volume_eligible, false)
            """,
            [scope],
        ).fetchone()
        conn.execute(
            f"""
            DELETE FROM {registry}
            WHERE scope_name = ?
              AND source IN ('events_api', 'markets_api')
              AND NOT coalesce(is_event_volume_eligible, false)
            """,
            [scope],
        )
    return int(count_row[0]) if count_row and count_row[0] is not None else 0


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
    # Latest membership is per (event_id, market_id). Related non-enclosing
    # bridges must not veto a still-current enclosing membership on another
    # eligible event. When several eligible events currently enclose a market,
    # keep the newest enclosing pair for the registry primary key.
    rows = conn.execute(
        f"""
        SELECT event_id, market_id
        FROM (
            SELECT
                event_id,
                market_id,
                row_number() OVER (
                    PARTITION BY market_id
                    ORDER BY observed_at DESC
                ) AS market_rn
            FROM (
                SELECT
                    event_id,
                    market_id,
                    is_enclosing_event,
                    observed_at,
                    row_number() OVER (
                        PARTITION BY event_id, market_id
                        ORDER BY observed_at DESC
                    ) AS event_market_rn
                FROM {event_market_snapshots}
                WHERE event_id IN ({placeholders})
            )
            WHERE event_market_rn = 1
              AND is_enclosing_event
        )
        WHERE market_rn = 1
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
        if not is_numeric_polymarket_id(event_id):
            continue
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
        market_id = membership["market_id"]
        event_id = membership["event_id"]
        if not is_numeric_polymarket_id(market_id) or not is_numeric_polymarket_id(
            event_id
        ):
            continue
        event = eligible_events[event_id]
        rows.append(
            RegistryRow(
                scope_name=scope,
                market_id=market_id,
                event_slug=event.get("event_slug"),
                event_id=event_id,
                source="event_catalog",
                event_volume_usd_lifetime_reported=event.get(
                    "event_volume_usd_lifetime_reported"
                ),
                is_event_volume_eligible=True,
                first_eligible_at=event.get("first_eligible_at"),
            )
        )
    rows.extend(
        row
        for row in seed_rows
        if is_numeric_polymarket_id(row.market_id)
        and (
            row.event_id is None
            or str(row.event_id).strip() == ""
            or is_numeric_polymarket_id(row.event_id)
        )
    )
    return rows


__all__ = [
    "RegistryRow",
    "build_registry_rows_from_event_catalog",
    "clear_registry",
    "get_registry_event_slugs",
    "get_registry_market_ids",
    "prune_ineligible_api_registry_rows",
    "prune_stale_event_catalog_registry_rows",
    "registry_market_count",
    "upsert_registry_rows",
]
