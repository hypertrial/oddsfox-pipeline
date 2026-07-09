"""Storage tests for Kalshi dlt batch landing helpers."""

from __future__ import annotations

from datetime import datetime, timezone

from oddsfox_pipeline.storage.duckdb import kalshi_dlt_batch
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    kalshi_ops_tbl,
    kalshi_raw_tbl,
)


def test_append_kalshi_pipeline_run_event_stage(duck):
    with duck.get_connection() as conn:
        kalshi_dlt_batch.append_kalshi_pipeline_run_event_stage(
            {
                "run_id": "run-1",
                "task_name": "sync_kalshi_candlesticks",
                "recorded_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "metrics_json": '{"rows_written": 1}',
            },
            conn,
        )
        row = conn.execute(
            f"""
            SELECT task_name, metrics_json
            FROM {kalshi_ops_tbl("wc2026", "pipeline_run_events")}
            WHERE run_id = 'run-1'
            """
        ).fetchone()
    assert row[0] == "sync_kalshi_candlesticks"
    assert '"rows_written": 1' in row[1]


def test_load_kalshi_market_scope_registry_stage_dedupes_latest(duck):
    with duck.get_connection() as conn:
        kalshi_dlt_batch.load_kalshi_market_scope_registry_stage(
            [
                {
                    "scope_name": "wc2026",
                    "market_ticker": "KXWC-MKT1",
                    "event_ticker": "KXWC-EVT-OLD",
                    "series_ticker": "KXWC",
                    "source": "seed",
                    "refreshed_at": datetime(2026, 1, 1),
                },
                {
                    "scope_name": "wc2026",
                    "market_ticker": "KXWC-MKT1",
                    "event_ticker": "KXWC-EVT-NEW",
                    "series_ticker": "KXWC",
                    "source": "series_api",
                    "refreshed_at": datetime(2026, 1, 2),
                },
            ],
            conn,
        )
        row = conn.execute(
            f"""
            SELECT event_ticker, source
            FROM {kalshi_ops_tbl("wc2026", "market_scope_registry")}
            WHERE market_ticker = 'KXWC-MKT1'
            """
        ).fetchone()
    assert row == ("KXWC-EVT-NEW", "series_api")


def test_load_kalshi_candlesticks_stage_dedupes_latest(duck):
    with duck.get_connection() as conn:
        kalshi_dlt_batch.load_kalshi_candlesticks_stage(
            [
                {
                    "market_ticker": "KXWC-MKT1",
                    "hour_start_utc": datetime(2026, 1, 1, 0, 0, 0),
                    "close_price": 0.3,
                    "refreshed_at": datetime(2026, 1, 1, 1, 0, 0),
                },
                {
                    "market_ticker": "KXWC-MKT1",
                    "hour_start_utc": datetime(2026, 1, 1, 0, 0, 0),
                    "close_price": 0.5,
                    "refreshed_at": datetime(2026, 1, 1, 2, 0, 0),
                },
            ],
            conn,
        )
        row = conn.execute(
            f"""
            SELECT close_price
            FROM {kalshi_raw_tbl("wc2026", "market_candlesticks_hourly")}
            WHERE market_ticker = 'KXWC-MKT1'
            """
        ).fetchone()
    assert row[0] == 0.5
