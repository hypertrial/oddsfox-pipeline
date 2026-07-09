"""Unit tests for Kalshi hourly candlestick fetch helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from oddsfox_pipeline.ingestion.kalshi.candlesticks import fetch as candlesticks_fetch


def test_fetch_hourly_candlesticks_normalizes_api_rows(monkeypatch):
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    client = MagicMock()
    monkeypatch.setattr(
        candlesticks_fetch,
        "fetch_market_candlesticks",
        lambda *_args, **_kwargs: [
            {
                "end_period_ts": 1_700_000_000,
                "price": {"close_dollars": "0.42"},
                "volume": 3,
            }
        ],
    )

    rows = candlesticks_fetch.fetch_hourly_candlesticks(
        client,
        series_ticker="KXMENWORLDCUP",
        market_ticker="KXMENWORLDCUP-WINNER-USA",
        start_at=start,
        end_at=end,
    )

    assert len(rows) == 1
    assert rows[0]["market_ticker"] == "KXMENWORLDCUP-WINNER-USA"
    assert rows[0]["close_price"] == 0.42


def test_to_epoch_seconds_treats_naive_datetimes_as_utc():
    naive = datetime(2026, 1, 1, 12, 0, 0)
    aware = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert candlesticks_fetch._to_epoch_seconds(
        naive
    ) == candlesticks_fetch._to_epoch_seconds(aware)
