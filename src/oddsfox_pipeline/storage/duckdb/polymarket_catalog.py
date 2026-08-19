"""Durable staging and atomic activation for the global Polymarket catalog."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Final

import duckdb

from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    POLYMARKET_CATALOG_OPS_SCHEMA,
    POLYMARKET_CATALOG_RAW_SCHEMA,
    polymarket_q,
)

CATALOG_RUNS: Final = polymarket_q(POLYMARKET_CATALOG_OPS_SCHEMA, "crawl_runs")
CATALOG_PAGES: Final = polymarket_q(POLYMARKET_CATALOG_OPS_SCHEMA, "crawl_pages")
CATALOG_ISSUES: Final = polymarket_q(POLYMARKET_CATALOG_OPS_SCHEMA, "crawl_issues")
EVENT_SNAPSHOTS: Final = polymarket_q(POLYMARKET_CATALOG_RAW_SCHEMA, "event_snapshots")
MARKET_SNAPSHOTS: Final = polymarket_q(
    POLYMARKET_CATALOG_RAW_SCHEMA, "market_snapshots"
)
EVENT_MARKET_SNAPSHOTS: Final = polymarket_q(
    POLYMARKET_CATALOG_RAW_SCHEMA, "event_market_snapshots"
)

_EVENT_COLUMNS = (
    "crawl_id",
    "observed_at",
    "event_id",
    "title",
    "subtitle",
    "description",
    "resolution_source",
    "slug",
    "category",
    "tags_json",
    "series_json",
    "is_active",
    "is_closed",
    "is_archived",
    "is_resolved",
    "source_created_at",
    "source_updated_at",
    "start_at",
    "end_at",
    "closed_at",
    "attributes_json",
    "content_text",
    "content_text_sha256",
)
_MARKET_COLUMNS = (
    "crawl_id",
    "observed_at",
    "market_id",
    "title",
    "subtitle",
    "description",
    "resolution_source",
    "slug",
    "category",
    "tags_json",
    "outcomes_json",
    "tradability_evidence_json",
    "is_active",
    "is_closed",
    "is_archived",
    "is_resolved",
    "is_tradable",
    "source_created_at",
    "source_updated_at",
    "start_at",
    "end_at",
    "closed_at",
    "condition_id",
    "attributes_json",
    "content_text",
    "content_text_sha256",
)
_EDGE_COLUMNS = (
    "crawl_id",
    "observed_at",
    "event_id",
    "market_id",
    "evidence_json",
    "content_text",
    "content_text_sha256",
)


def ensure_catalog_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{POLYMARKET_CATALOG_RAW_SCHEMA}"')
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{POLYMARKET_CATALOG_OPS_SCHEMA}"')
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_RUNS} (
            crawl_id VARCHAR PRIMARY KEY,
            observed_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status VARCHAR NOT NULL CHECK (status IN ('running', 'failed', 'complete')),
            error_type VARCHAR,
            summary_json VARCHAR
        );
        CREATE TABLE IF NOT EXISTS {CATALOG_PAGES} (
            crawl_id VARCHAR NOT NULL,
            pass_name VARCHAR NOT NULL,
            page_number INTEGER NOT NULL,
            payload_json VARCHAR NOT NULL,
            next_cursor VARCHAR,
            is_complete BOOLEAN NOT NULL,
            PRIMARY KEY (crawl_id, pass_name, page_number)
        );
        CREATE TABLE IF NOT EXISTS {CATALOG_ISSUES} (
            issue_id VARCHAR PRIMARY KEY,
            crawl_id VARCHAR NOT NULL,
            issue_type VARCHAR NOT NULL,
            detail VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS {EVENT_SNAPSHOTS} (
            crawl_id VARCHAR NOT NULL, observed_at TIMESTAMP NOT NULL,
            event_id VARCHAR NOT NULL, title VARCHAR, subtitle VARCHAR,
            description VARCHAR, resolution_source VARCHAR, slug VARCHAR,
            category VARCHAR, tags_json VARCHAR NOT NULL, series_json VARCHAR NOT NULL,
            is_active BOOLEAN, is_closed BOOLEAN, is_archived BOOLEAN,
            is_resolved BOOLEAN, source_created_at TIMESTAMP,
            source_updated_at TIMESTAMP, start_at TIMESTAMP, end_at TIMESTAMP,
            closed_at TIMESTAMP, attributes_json VARCHAR NOT NULL,
            content_text VARCHAR NOT NULL, content_text_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (crawl_id, event_id)
        );
        CREATE TABLE IF NOT EXISTS {MARKET_SNAPSHOTS} (
            crawl_id VARCHAR NOT NULL, observed_at TIMESTAMP NOT NULL,
            market_id VARCHAR NOT NULL, title VARCHAR, subtitle VARCHAR,
            description VARCHAR, resolution_source VARCHAR, slug VARCHAR,
            category VARCHAR, tags_json VARCHAR NOT NULL, outcomes_json VARCHAR NOT NULL,
            tradability_evidence_json VARCHAR NOT NULL, is_active BOOLEAN,
            is_closed BOOLEAN, is_archived BOOLEAN, is_resolved BOOLEAN,
            is_tradable BOOLEAN NOT NULL, source_created_at TIMESTAMP,
            source_updated_at TIMESTAMP, start_at TIMESTAMP, end_at TIMESTAMP,
            closed_at TIMESTAMP, condition_id VARCHAR, attributes_json VARCHAR NOT NULL,
            content_text VARCHAR NOT NULL, content_text_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (crawl_id, market_id)
        );
        CREATE TABLE IF NOT EXISTS {EVENT_MARKET_SNAPSHOTS} (
            crawl_id VARCHAR NOT NULL, observed_at TIMESTAMP NOT NULL,
            event_id VARCHAR NOT NULL, market_id VARCHAR NOT NULL,
            evidence_json VARCHAR NOT NULL, content_text VARCHAR NOT NULL,
            content_text_sha256 VARCHAR NOT NULL,
            PRIMARY KEY (crawl_id, event_id, market_id)
        );
        """
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def start_catalog_crawl(
    conn: duckdb.DuckDBPyConnection,
    crawl_id: str,
    *,
    error_type: str | None = None,
) -> str:
    ensure_catalog_tables(conn)
    row = conn.execute(
        f"SELECT observed_at, status FROM {CATALOG_RUNS} WHERE crawl_id = ?",
        [crawl_id],
    ).fetchone()
    now = _utc_now()
    if row is None:
        conn.execute(
            f"INSERT INTO {CATALOG_RUNS} VALUES (?, ?, ?, NULL, ?, ?, NULL)",
            [crawl_id, now, now, "failed" if error_type else "running", error_type],
        )
        observed_at = now
    else:
        observed_at = row[0]
        if row[1] != "complete":
            conn.execute(
                f"UPDATE {CATALOG_RUNS} SET status = ?, error_type = ? WHERE crawl_id = ?",
                ["failed" if error_type else "running", error_type, crawl_id],
            )
    return observed_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def catalog_crawl_status(
    conn: duckdb.DuckDBPyConnection, crawl_id: str
) -> dict[str, Any] | None:
    ensure_catalog_tables(conn)
    row = conn.execute(
        f"SELECT status, summary_json FROM {CATALOG_RUNS} WHERE crawl_id = ?",
        [crawl_id],
    ).fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "summary": json.loads(row[1]) if row[1] else None,
    }


def catalog_crawl_pages(
    conn: duckdb.DuckDBPyConnection, crawl_id: str, pass_name: str
) -> list[dict[str, Any]]:
    ensure_catalog_tables(conn)
    rows = conn.execute(
        f"""
        SELECT pass_name, page_number, payload_json, next_cursor, is_complete
        FROM {CATALOG_PAGES}
        WHERE crawl_id = ? AND pass_name = ?
        ORDER BY page_number
        """,
        [crawl_id, pass_name],
    ).fetchall()
    return [
        dict(
            zip(
                (
                    "pass_name",
                    "page_number",
                    "payload_json",
                    "next_cursor",
                    "is_complete",
                ),
                row,
                strict=True,
            )
        )
        for row in rows
    ]


def save_catalog_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    crawl_id: str,
    pass_name: str,
    page_number: int,
    payload: Mapping[str, Any],
    next_cursor: str | None,
    is_complete: bool,
) -> None:
    payload_json = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    existing = conn.execute(
        f"SELECT payload_json, next_cursor, is_complete FROM {CATALOG_PAGES} WHERE crawl_id = ? AND pass_name = ? AND page_number = ?",
        [crawl_id, pass_name, page_number],
    ).fetchone()
    candidate = (payload_json, next_cursor, is_complete)
    if existing is not None and existing != candidate:
        raise RuntimeError("catalog page replay diverged")
    if existing is None:
        conn.execute(
            f"INSERT INTO {CATALOG_PAGES} VALUES (?, ?, ?, ?, ?, ?)",
            [crawl_id, pass_name, page_number, *candidate],
        )


def delete_catalog_pass(
    conn: duckdb.DuckDBPyConnection, crawl_id: str, pass_name: str
) -> None:
    conn.execute(
        f"DELETE FROM {CATALOG_PAGES} WHERE crawl_id = ? AND pass_name = ?",
        [crawl_id, pass_name],
    )


def record_catalog_issue(
    conn: duckdb.DuckDBPyConnection,
    *,
    crawl_id: str,
    issue_type: str,
    detail: str,
) -> None:
    """Persist a deterministic, source-text-free quarantine issue."""
    issue_id = hashlib.sha256(
        f"{crawl_id}\x00{issue_type}\x00{detail}".encode()
    ).hexdigest()
    conn.execute(
        f"INSERT OR IGNORE INTO {CATALOG_ISSUES} VALUES (?, ?, ?, ?, ?)",
        [issue_id, crawl_id, issue_type, detail, _utc_now()],
    )


def _insert_rows(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    if not rows:
        return
    names = ", ".join(f'"{name}"' for name in columns)
    placeholders = ", ".join("?" for _ in columns)
    conn.executemany(
        f"INSERT INTO {table} ({names}) VALUES ({placeholders})",
        [[row.get(column) for column in columns] for row in rows],
    )


def activate_catalog_crawl(
    conn: duckdb.DuckDBPyConnection,
    *,
    crawl_id: str,
    event_rows: Sequence[Mapping[str, Any]],
    market_rows: Sequence[Mapping[str, Any]],
    edge_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ensure_catalog_tables(conn)
    issue_count = int(
        conn.execute(
            f"SELECT count(*) FROM {CATALOG_ISSUES} WHERE crawl_id = ?", [crawl_id]
        ).fetchone()[0]
    )
    if issue_count:
        raise RuntimeError("catalog crawl has quarantined issues and cannot activate")
    pass_rows = conn.execute(
        f"""
        SELECT pass_name, count(*) AS pages,
               count(*) FILTER (WHERE is_complete) AS completed_pages,
               sum(json_array_length(
                   json_extract(payload_json,
                       CASE WHEN starts_with(pass_name, 'events_')
                            THEN '$.events' ELSE '$.markets' END)
               )) AS source_rows
        FROM {CATALOG_PAGES} WHERE crawl_id = ? GROUP BY pass_name
        """,
        [crawl_id],
    ).fetchall()
    pass_inventory = {
        name: {
            "pages": int(pages),
            "source_rows": int(source_rows or 0),
            "complete": completed == 1,
        }
        for name, pages, completed, source_rows in pass_rows
    }
    expected = {"events_open", "events_closed", "markets_open", "markets_closed"}
    if set(pass_inventory) != expected or not all(
        item["complete"] for item in pass_inventory.values()
    ):
        raise RuntimeError("all four catalog passes must complete before activation")
    completed_at = _utc_now()
    summary = {
        "crawl_id": crawl_id,
        "events": len(event_rows),
        "markets": len(market_rows),
        "qualifying_markets": sum(bool(row.get("is_tradable")) for row in market_rows),
        "event_market_memberships": len(edge_rows),
        "passes": pass_inventory,
        "completed_at": completed_at.replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
    }
    conn.execute("BEGIN")
    try:
        for table in (EVENT_SNAPSHOTS, MARKET_SNAPSHOTS, EVENT_MARKET_SNAPSHOTS):
            conn.execute(f"DELETE FROM {table} WHERE crawl_id = ?", [crawl_id])
        _insert_rows(conn, EVENT_SNAPSHOTS, _EVENT_COLUMNS, event_rows)
        _insert_rows(conn, MARKET_SNAPSHOTS, _MARKET_COLUMNS, market_rows)
        _insert_rows(conn, EVENT_MARKET_SNAPSHOTS, _EDGE_COLUMNS, edge_rows)
        conn.execute(
            f"""
            UPDATE {CATALOG_RUNS}
            SET status = 'complete', completed_at = ?, error_type = NULL,
                summary_json = ?
            WHERE crawl_id = ?
            """,
            [
                completed_at,
                json.dumps(summary, sort_keys=True, separators=(",", ":")),
                crawl_id,
            ],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return summary


__all__ = [
    "CATALOG_PAGES",
    "CATALOG_ISSUES",
    "CATALOG_RUNS",
    "EVENT_MARKET_SNAPSHOTS",
    "EVENT_SNAPSHOTS",
    "MARKET_SNAPSHOTS",
    "activate_catalog_crawl",
    "catalog_crawl_pages",
    "catalog_crawl_status",
    "delete_catalog_pass",
    "ensure_catalog_tables",
    "record_catalog_issue",
    "save_catalog_page",
    "start_catalog_crawl",
]
