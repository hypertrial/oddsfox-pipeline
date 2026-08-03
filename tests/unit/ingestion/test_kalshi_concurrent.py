from unittest.mock import MagicMock

import pytest
import requests

from oddsfox_pipeline.ingestion.kalshi.candlesticks import sync as candlesticks_sync
from oddsfox_pipeline.ingestion.kalshi.concurrent import map_bounded
from oddsfox_pipeline.storage.duckdb.profile.discovery import _classify_warehouse_type


def test_map_bounded_skips_failed_workers():
    def worker(item: int) -> int:
        if item == 2:
            raise requests.exceptions.ConnectionError("simulated")
        return item

    assert map_bounded([1, 2, 3], worker, max_workers=2) == [1, 3]


def test_sync_hourly_candlesticks_continues_after_market_failure(monkeypatch):
    candlesticks_sync.get_registry_markets_for_sync = lambda **_: [
        {"market_ticker": f"KXWC-MKT{i}", "series_ticker": "KXWC", "open_time": None}
        for i in range(3)
    ]

    def fake_fetch(client, *, series_ticker, market_ticker, start_at, end_at):
        if market_ticker == "KXWC-MKT1":
            raise requests.exceptions.ConnectionError("simulated")
        return []

    candlesticks_sync.fetch_hourly_candlesticks = fake_fetch
    monkeypatch.setattr(
        candlesticks_sync,
        "save_candlesticks_batch",
        lambda rows: len(rows),
    )
    monkeypatch.setattr(
        candlesticks_sync, "upsert_candlestick_ledger_state", lambda **_: None
    )
    monkeypatch.setattr(
        candlesticks_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )

    metrics = candlesticks_sync.sync_hourly_candlesticks(
        scope_name="wc2026",
        force=True,
        client_factory=lambda: MagicMock(),
    )

    assert metrics["markets_total"] == 3
    assert metrics["empty_markets"] == 2


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        ("UBIGINT", "numeric"),
        ("UINTEGER", "numeric"),
        ("USMALLINT", "numeric"),
        ("UTINYINT", "numeric"),
        ("BIGINT", "numeric"),
    ],
)
def test_classify_warehouse_type_covers_unsigned_integers(data_type, expected):
    assert _classify_warehouse_type(data_type) == expected
