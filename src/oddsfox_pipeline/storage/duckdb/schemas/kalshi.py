"""Kalshi raw/ops DDL, primary keys, and indexes."""

from __future__ import annotations

import logging

import duckdb

from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    kalshi_ops_tbl,
    kalshi_raw_schema,
    kalshi_raw_tbl,
)

logger = logging.getLogger(__name__)

_KALSHI_SCOPES = (SCOPE_WC2026,)


def ensure_kalshi_indexes(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    m = kalshi_raw_tbl(scope_name, "markets")
    e = kalshi_raw_tbl(scope_name, "events")
    c = kalshi_raw_tbl(scope_name, "market_candlesticks_hourly")
    scope_reg = kalshi_ops_tbl(scope_name, "market_scope_registry")
    led = kalshi_ops_tbl(scope_name, "candlestick_sync_ledger")
    raw_schema = kalshi_raw_schema(scope_name)
    index_statements = [
        "CREATE INDEX IF NOT EXISTS "
        f"idx_{scope_name}_kalshi_scope_registry_scope_event ON {scope_reg}"
        "(scope_name, event_ticker)",
        "CREATE INDEX IF NOT EXISTS "
        f"idx_{scope_name}_kalshi_scope_registry_market ON {scope_reg}(market_ticker)",
        f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_kalshi_candlesticks_ticker ON {c}(market_ticker)",
        f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_kalshi_candlesticks_hour ON {c}(hour_start_utc)",
        f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_kalshi_candlestick_ledger ON {led}(market_ticker)",
    ]
    for table_name, col in (("markets", "event_ticker"), ("events", "series_ticker")):
        exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [raw_schema, table_name],
        ).fetchone()
        if exists and exists[0]:
            tbl = kalshi_raw_tbl(scope_name, table_name)
            index_statements.append(
                f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_kalshi_{table_name}_{col} "
                f"ON {tbl}({col})"
            )
    markets_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = 'markets'
        """,
        [raw_schema],
    ).fetchone()
    if markets_exists and markets_exists[0]:
        index_statements.append(
            f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_kalshi_market_ticker ON {m}(market_ticker)"
        )
        index_statements.append(
            f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_kalshi_event_ticker ON {e}(event_ticker)"
        )
    for stmt in index_statements:
        try:
            conn.execute(stmt)
        except Exception as exc:
            logger.warning("Index statement skipped (%s): %s", stmt, exc)


def bootstrap_kalshi_tables(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    """CREATE TABLE IF NOT EXISTS for Kalshi warehouse tables.

    ``{scope}_raw.events`` and ``{scope}_raw.markets`` are owned by dlt landing.
    """
    pre = kalshi_ops_tbl(scope_name, "pipeline_run_events")
    srm = kalshi_ops_tbl(scope_name, "sync_run_metrics")
    scope_reg = kalshi_ops_tbl(scope_name, "market_scope_registry")
    led = kalshi_ops_tbl(scope_name, "candlestick_sync_ledger")
    c = kalshi_raw_tbl(scope_name, "market_candlesticks_hourly")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {pre} (
            run_id TEXT PRIMARY KEY,
            task_name TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            metrics_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {srm} (
            task_name TEXT PRIMARY KEY,
            recorded_at TIMESTAMP NOT NULL,
            metrics_json TEXT NOT NULL,
            history_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {scope_reg} (
            scope_name TEXT,
            market_ticker TEXT,
            event_ticker TEXT,
            series_ticker TEXT,
            source TEXT,
            refreshed_at TIMESTAMP,
            PRIMARY KEY (scope_name, market_ticker)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {led} (
            market_ticker TEXT PRIMARY KEY,
            last_sync_hour_start BIGINT,
            fully_checked BOOLEAN DEFAULT FALSE,
            last_checked_at TIMESTAMP,
            next_check_at TIMESTAMP,
            empty_run_streak INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {c} (
            market_ticker TEXT,
            hour_start_utc TIMESTAMP,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            avg_price DOUBLE,
            volume BIGINT,
            refreshed_at TIMESTAMP,
            PRIMARY KEY (market_ticker, hour_start_utc)
        )
        """
    )


def bootstrap_all_kalshi_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for scope_name in _KALSHI_SCOPES:
        bootstrap_kalshi_tables(conn, scope_name=scope_name)


def ensure_all_kalshi_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    for scope_name in _KALSHI_SCOPES:
        ensure_kalshi_indexes(conn, scope_name=scope_name)


def create_test_kalshi_raw_tables(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    """Empty Kalshi raw fixtures for dbt source tests and local CI."""
    events = kalshi_raw_tbl(scope_name, "events")
    markets = kalshi_raw_tbl(scope_name, "markets")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {events} (
            event_ticker TEXT PRIMARY KEY,
            series_ticker TEXT,
            title TEXT,
            sub_title TEXT,
            category TEXT,
            status TEXT,
            open_time TIMESTAMP,
            close_time TIMESTAMP,
            scraped_at TIMESTAMP
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {markets} (
            market_ticker TEXT PRIMARY KEY,
            event_ticker TEXT,
            series_ticker TEXT,
            title TEXT,
            subtitle TEXT,
            yes_sub_title TEXT,
            no_sub_title TEXT,
            status TEXT,
            market_type TEXT,
            open_time TIMESTAMP,
            close_time TIMESTAMP,
            expiration_time TIMESTAMP,
            volume BIGINT,
            open_interest BIGINT,
            last_price_dollars TEXT,
            scraped_at TIMESTAMP
        )
        """
    )


def create_all_kalshi_test_raw_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for scope_name in _KALSHI_SCOPES:
        create_test_kalshi_raw_tables(conn, scope_name=scope_name)


def seed_test_kalshi_pipeline_run_event(conn: duckdb.DuckDBPyConnection) -> None:
    """Healthy sync_kalshi_candlesticks fixture for dbt observability tests."""
    import json
    import uuid
    from datetime import datetime, timezone

    pre = kalshi_ops_tbl(SCOPE_WC2026, "pipeline_run_events")
    recorded_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    metrics = {
        "scope_name": "wc2026",
        "markets_synced": 1,
        "rows_written": 10,
        "window_hours": 1512,
    }
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {pre} (
            run_id,
            task_name,
            recorded_at,
            metrics_json
        )
        VALUES (?, ?, ?, ?)
        """,
        [
            str(uuid.uuid4()),
            "sync_kalshi_candlesticks",
            recorded_at,
            json.dumps(metrics, sort_keys=True),
        ],
    )


__all__ = [
    "bootstrap_all_kalshi_tables",
    "bootstrap_kalshi_tables",
    "create_all_kalshi_test_raw_tables",
    "create_test_kalshi_raw_tables",
    "ensure_all_kalshi_indexes",
    "ensure_kalshi_indexes",
    "seed_test_kalshi_pipeline_run_event",
]
