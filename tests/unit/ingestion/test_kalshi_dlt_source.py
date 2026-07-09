"""Unit tests for Kalshi dlt source resources."""

from __future__ import annotations

from oddsfox_pipeline.ingestion.kalshi.dlt_source import (
    collect_raw_events_and_markets,
    kalshi_wc2026_source,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch import DLT_STRICT_SCHEMA_CONTRACT


def test_kalshi_wc2026_source_yields_prefetched_rows():
    events = [{"event_ticker": "KXWC-EVT1", "series_ticker": "KXWC", "scraped_at": "x"}]
    markets = [
        {
            "market_ticker": "KXWC-MKT1",
            "event_ticker": "KXWC-EVT1",
            "series_ticker": "KXWC",
            "scraped_at": "x",
        }
    ]
    source = kalshi_wc2026_source(events=events, markets=markets)

    assert list(source.resources["events"]) == events
    assert list(source.resources["markets"]) == markets


def test_kalshi_resources_have_frozen_contract():
    source = kalshi_wc2026_source()
    events = source.resources["events"]
    markets = source.resources["markets"]

    assert events.schema_contract == DLT_STRICT_SCHEMA_CONTRACT
    assert markets.schema_contract == DLT_STRICT_SCHEMA_CONTRACT
    assert events.columns["event_ticker"]["data_type"] == "text"
    assert markets.columns["market_ticker"]["data_type"] == "text"


def test_collect_raw_events_and_markets_delegates(monkeypatch):
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.kalshi.dlt_source.collect_market_scope_payload",
        lambda **kwargs: {
            "events": [{"event_ticker": "KXWC-EVT1"}],
            "markets": [{"market_ticker": "KXWC-MKT1"}],
            **kwargs,
        },
    )

    payload = collect_raw_events_and_markets(scope_name="wc2026")

    assert payload["events"][0]["event_ticker"] == "KXWC-EVT1"
    assert payload["markets"][0]["market_ticker"] == "KXWC-MKT1"
