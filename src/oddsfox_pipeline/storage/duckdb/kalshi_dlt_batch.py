"""dlt batch landing helpers for Kalshi DuckDB tables."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import duckdb

from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    PIPELINE_RUN_EVENT_COLUMNS,
    load_stage_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    kalshi_ops_schema,
    kalshi_ops_tbl,
    kalshi_raw_schema,
    kalshi_raw_tbl,
)

KALSHI_MARKET_SCOPE_REGISTRY_COLUMNS = {
    "scope_name": {"data_type": "text"},
    "market_ticker": {"data_type": "text"},
    "event_ticker": {"data_type": "text"},
    "series_ticker": {"data_type": "text"},
    "source": {"data_type": "text"},
    "refreshed_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

KALSHI_CANDLESTICK_COLUMNS = {
    "market_ticker": {"data_type": "text"},
    "hour_start_utc": {"data_type": "timestamp"},
    "open_price": {"data_type": "double", "nullable": True},
    "high_price": {"data_type": "double", "nullable": True},
    "low_price": {"data_type": "double", "nullable": True},
    "close_price": {"data_type": "double", "nullable": True},
    "avg_price": {"data_type": "double", "nullable": True},
    "volume": {"data_type": "bigint", "nullable": True},
    "refreshed_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}


def _with_row_order(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "row_order": idx} for idx, row in enumerate(rows)]


def append_kalshi_pipeline_run_event_stage(
    row: dict[str, Any],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    ops_schema = kalshi_ops_schema(scope_name)
    target = kalshi_ops_tbl(scope_name, "pipeline_run_events")
    stage = load_stage_rows(
        schema=ops_schema,
        stage_table="stage_kalshi_pipeline_run_events_v1",
        rows=[row],
        columns=PIPELINE_RUN_EVENT_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (run_id, task_name, recorded_at, metrics_json)
        SELECT run_id, task_name, recorded_at, metrics_json
        FROM {stage}
        """
    )


def load_kalshi_market_scope_registry_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    ops_schema = kalshi_ops_schema(scope_name)
    target = kalshi_ops_tbl(scope_name, "market_scope_registry")
    stage = load_stage_rows(
        schema=ops_schema,
        stage_table="stage_kalshi_market_scope_registry_v1",
        rows=_with_row_order(rows),
        columns=KALSHI_MARKET_SCOPE_REGISTRY_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (scope_name, market_ticker, event_ticker, series_ticker, source, refreshed_at)
        SELECT scope_name, market_ticker, event_ticker, series_ticker, source, refreshed_at
        FROM (
            SELECT
                scope_name,
                market_ticker,
                event_ticker,
                series_ticker,
                source,
                refreshed_at,
                row_number() OVER (
                    PARTITION BY scope_name, market_ticker
                    ORDER BY refreshed_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        ON CONFLICT(scope_name, market_ticker) DO UPDATE SET
          event_ticker=excluded.event_ticker,
          series_ticker=excluded.series_ticker,
          source=excluded.source,
          refreshed_at=excluded.refreshed_at
        """
    )


def load_kalshi_candlesticks_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    raw_schema = kalshi_raw_schema(scope_name)
    target = kalshi_raw_tbl(scope_name, "market_candlesticks_hourly")
    stage = load_stage_rows(
        schema=raw_schema,
        stage_table="stage_kalshi_candlesticks_hourly_v1",
        rows=_with_row_order(rows),
        columns=KALSHI_CANDLESTICK_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (
            market_ticker,
            hour_start_utc,
            open_price,
            high_price,
            low_price,
            close_price,
            avg_price,
            volume,
            refreshed_at
        )
        SELECT
            market_ticker,
            hour_start_utc,
            open_price,
            high_price,
            low_price,
            close_price,
            avg_price,
            volume,
            refreshed_at
        FROM (
            SELECT
                market_ticker,
                hour_start_utc,
                open_price,
                high_price,
                low_price,
                close_price,
                avg_price,
                volume,
                refreshed_at,
                row_number() OVER (
                    PARTITION BY market_ticker, hour_start_utc
                    ORDER BY refreshed_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        ON CONFLICT(market_ticker, hour_start_utc) DO UPDATE SET
          open_price=excluded.open_price,
          high_price=excluded.high_price,
          low_price=excluded.low_price,
          close_price=excluded.close_price,
          avg_price=excluded.avg_price,
          volume=excluded.volume,
          refreshed_at=excluded.refreshed_at
        """
    )


__all__ = [
    "KALSHI_CANDLESTICK_COLUMNS",
    "KALSHI_MARKET_SCOPE_REGISTRY_COLUMNS",
    "append_kalshi_pipeline_run_event_stage",
    "load_kalshi_candlesticks_stage",
    "load_kalshi_market_scope_registry_stage",
]
