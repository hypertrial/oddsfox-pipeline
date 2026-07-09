"""Unit tests for Kalshi hourly candlestick sync."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from oddsfox_pipeline.ingestion.kalshi.candlesticks import sync as candlesticks_sync
from oddsfox_pipeline.storage.duckdb.kalshi_market_scope_registry import (
    KalshiRegistryRow,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import kalshi_raw_tbl
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import create_test_kalshi_raw_tables


def _seed_market(duck, *, market_ticker, open_time=None):
    with duck.get_connection() as conn:
        create_test_kalshi_raw_tables(conn)
    upsert_registry_rows(
        [
            KalshiRegistryRow(
                market_ticker=market_ticker,
                event_ticker="KXWC-EVT1",
                series_ticker="KXWC",
                source="test",
            )
        ]
    )
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {kalshi_raw_tbl("wc2026", "markets")} (
                market_ticker, event_ticker, series_ticker, open_time, scraped_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                market_ticker,
                "KXWC-EVT1",
                "KXWC",
                open_time,
                datetime(2026, 1, 1),
            ],
        )


def test_sync_hourly_candlesticks_writes_rows_and_metrics(monkeypatch, duck):
    _seed_market(
        duck,
        market_ticker="KXMENWORLDCUP-WINNER-USA",
        open_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    progress = []
    monkeypatch.setattr(
        candlesticks_sync,
        "fetch_hourly_candlesticks",
        lambda *_args, **_kwargs: [
            {
                "market_ticker": "KXMENWORLDCUP-WINNER-USA",
                "hour_start_utc": datetime(2026, 1, 1),
                "close_price": 0.5,
                "refreshed_at": datetime(2026, 1, 1),
            }
        ],
    )
    monkeypatch.setattr(
        candlesticks_sync,
        "_utc_now",
        lambda: datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    metrics = candlesticks_sync.sync_hourly_candlesticks(
        scope_name="wc2026",
        window_hours=24,
        client_factory=lambda: MagicMock(),
        progress_callback=lambda phase, payload: progress.append((phase, payload)),
    )

    assert metrics["markets_synced"] == 1
    assert metrics["rows_written"] == 1
    assert metrics["empty_markets"] == 0
    assert progress and progress[0][0] == "kalshi_candlesticks"


def test_sync_hourly_candlesticks_uses_naive_open_time(monkeypatch, duck):
    _seed_market(
        duck,
        market_ticker="KXWC-NAIVE-OPEN",
        open_time=datetime(2025, 6, 1, 0, 0, 0),
    )
    monkeypatch.setattr(
        candlesticks_sync,
        "fetch_hourly_candlesticks",
        lambda *_args, **_kwargs: [],
    )

    metrics = candlesticks_sync.sync_hourly_candlesticks(
        scope_name="wc2026",
        force=True,
        client_factory=lambda: MagicMock(),
    )

    assert metrics["markets_total"] == 1


def test_sync_hourly_candlesticks_counts_empty_markets(monkeypatch, duck):
    _seed_market(duck, market_ticker="KXWC-MKT1")
    monkeypatch.setattr(
        candlesticks_sync,
        "fetch_hourly_candlesticks",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        candlesticks_sync,
        "_utc_now",
        lambda: datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    metrics = candlesticks_sync.sync_hourly_candlesticks(
        scope_name="wc2026",
        force=True,
        client_factory=lambda: MagicMock(),
    )

    assert metrics["markets_synced"] == 0
    assert metrics["empty_markets"] == 1
    assert metrics["rows_written"] == 0


def test_sync_hourly_candlesticks_skips_open_time_when_missing(monkeypatch, duck):
    _seed_market(duck, market_ticker="KXWC-NO-OPEN", open_time=None)
    monkeypatch.setattr(
        candlesticks_sync,
        "fetch_hourly_candlesticks",
        lambda *_args, **_kwargs: [],
    )

    metrics = candlesticks_sync.sync_hourly_candlesticks(
        scope_name="wc2026",
        force=True,
        client_factory=lambda: MagicMock(),
    )

    assert metrics["markets_total"] == 1


def test_sync_hourly_candlesticks_honors_timezone_aware_open_time(monkeypatch):
    seen = {}

    def fake_get_registry(**_kwargs):
        return [
            {
                "market_ticker": "KXWC-AWARE",
                "series_ticker": "KXWC",
                "open_time": datetime(2025, 6, 1, tzinfo=timezone.utc),
            }
        ]

    monkeypatch.setattr(
        candlesticks_sync, "get_registry_markets_for_sync", fake_get_registry
    )
    monkeypatch.setattr(
        candlesticks_sync,
        "fetch_hourly_candlesticks",
        lambda *_args, **kwargs: seen.update(kwargs) or [],
    )
    monkeypatch.setattr(
        candlesticks_sync,
        "_utc_now",
        lambda: datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    candlesticks_sync.sync_hourly_candlesticks(
        scope_name="wc2026",
        force=True,
        client_factory=lambda: MagicMock(),
    )

    assert seen["start_at"] >= datetime(2025, 6, 1, tzinfo=timezone.utc)
