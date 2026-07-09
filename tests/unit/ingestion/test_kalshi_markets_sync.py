"""Unit tests for Kalshi markets sync orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock

from oddsfox_pipeline.ingestion.kalshi.markets import sync as markets_sync
from oddsfox_pipeline.ingestion.kalshi.series_scope.registry import KalshiCollectResult
from oddsfox_pipeline.storage.duckdb import metadata


def test_collect_market_scope_payload_normalizes_rows(monkeypatch):
    monkeypatch.setattr(
        markets_sync,
        "refresh_registry_and_collect",
        lambda _client, **kwargs: KalshiCollectResult(
            events=[{"event_ticker": "KXWC-EVT1", "series_ticker": "KXWC"}],
            markets=[
                {
                    "ticker": "KXWC-MKT1",
                    "event_ticker": "KXWC-EVT1",
                    "title": "Team A",
                }
            ],
            registry_rows=[],
            summary={"registry_rows_upserted": 1},
        ),
    )
    monkeypatch.setattr(
        markets_sync,
        "load_market_scope_config",
        lambda **kwargs: type(
            "Cfg",
            (),
            {"scope_name": "wc2026", "series_tickers": ("KXWC",)},
        )(),
    )

    payload = markets_sync.collect_market_scope_payload(
        client_factory=lambda: MagicMock(),
        scope_name="wc2026",
    )

    assert payload["scope_name"] == "wc2026"
    assert payload["total_events"] == 1
    assert payload["total_markets"] == 1
    assert payload["events"][0]["event_ticker"] == "KXWC-EVT1"
    assert payload["markets"][0]["market_ticker"] == "KXWC-MKT1"


def test_sync_markets_persists_metrics(monkeypatch, duck):
    monkeypatch.setattr(
        markets_sync,
        "collect_market_scope_payload",
        lambda **_kwargs: {
            "scope_name": "wc2026",
            "events": [],
            "markets": [],
            "registry_summary": {"registry_rows_upserted": 0},
            "total_events": 0,
            "total_markets": 0,
        },
    )

    payload = markets_sync.sync_markets(scope_name="wc2026")

    assert payload["scope_name"] == "wc2026"
    saved = metadata.get_sync_run_metrics("sync_kalshi_markets", source="kalshi")
    assert saved is not None
    assert saved["scope_name"] == "wc2026"
