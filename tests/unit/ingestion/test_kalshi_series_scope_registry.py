"""Unit tests for Kalshi series-scope registry refresh."""

from __future__ import annotations

from unittest.mock import MagicMock

from oddsfox_pipeline.ingestion.kalshi.series_scope import registry as series_registry
from oddsfox_pipeline.ingestion.kalshi.series_scope.config import (
    KalshiMarketScopeConfig,
)


def test_series_ticker_from_market_prefers_event_ticker_prefix():
    assert (
        series_registry._series_ticker_from_market(
            {"ticker": "KXWC-MKT1", "event_ticker": "KXWC-EVT1"}
        )
        == "KXWC"
    )
    assert (
        series_registry._series_ticker_from_market({"ticker": "SERIES-MKT1"})
        == "SERIES"
    )
    assert series_registry._series_ticker_from_market({"ticker": "NODASH"}) == ""


def test_refresh_registry_and_collect_filters_and_upserts(monkeypatch, duck):
    cfg = KalshiMarketScopeConfig(
        scope_name="wc2026",
        series_tickers=("KXWC",),
        excluded_market_suffixes={"KXWC": ("FW",)},
    )
    client = MagicMock()
    progress = []

    def fake_events(_client, series_ticker, *, progress_callback=None):
        assert series_ticker == "KXWC"
        if progress_callback:
            progress_callback("kalshi_page", {"pages": 1})
        return [
            {"event_ticker": "KXWC-EVT1"},
            {"event_ticker": ""},
        ]

    def fake_markets(_client, event_ticker, *, progress_callback=None):
        if event_ticker == "KXWC-EVT1":
            return [
                {"ticker": "KXWC-MKT1", "event_ticker": "KXWC-EVT1"},
                {"ticker": "KXWC-MKT-FW", "event_ticker": "KXWC-EVT1"},
                {"ticker": "", "event_ticker": "KXWC-EVT1"},
            ]
        return []

    monkeypatch.setattr(series_registry, "fetch_events_for_series", fake_events)
    monkeypatch.setattr(series_registry, "fetch_markets_for_event", fake_markets)

    result = series_registry.refresh_registry_and_collect(
        client,
        config=cfg,
        progress_callback=lambda phase, payload: progress.append((phase, payload)),
    )

    assert len(result.events) == 1
    assert [market["ticker"] for market in result.markets] == ["KXWC-MKT1"]
    assert result.summary["registry_upserted"] == 1
    assert result.summary["markets_collected"] == 1
    assert progress
