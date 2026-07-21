from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
import vcr

from oddsfox_pipeline.ingestion.kalshi.client import (
    fetch_events_for_series,
    fetch_market_candlesticks,
    fetch_markets_for_event,
)
from oddsfox_pipeline.ingestion.kalshi.markets.transform import (
    normalize_candlestick_rows,
    normalize_event_row,
    normalize_market_row,
)
from oddsfox_pipeline.ingestion.polymarket.gamma_events import fetch_gamma_event_by_slug
from oddsfox_pipeline.ingestion.polymarket.markets.transform import (
    process_markets_dataframe,
)
from oddsfox_pipeline.ingestion.polymarket.odds.fetch import fetch_token_history
from oddsfox_pipeline.resources.http import APIClient

pytestmark = pytest.mark.contract

CASSETTES = Path(__file__).resolve().parents[1] / "fixtures" / "cassettes"


def _replay_vcr() -> vcr.VCR:
    return vcr.VCR(
        cassette_library_dir=str(CASSETTES),
        decode_compressed_response=True,
        filter_headers=["authorization", "kalshi-access-key", "kalshi-signature"],
        match_on=("method", "scheme", "host", "port", "path", "query"),
        record_mode="none",
    )


def test_polymarket_gamma_market_and_event_payload_replay_contract():
    client = APIClient("https://gamma-api.polymarket.com", retries=0)

    with _replay_vcr().use_cassette("polymarket_gamma_market_event.yml"):
        market = client.get("/markets/pm-wc-arg-win")
        event = fetch_gamma_event_by_slug(client, "2026-fifa-world-cup-winner")

    df = process_markets_dataframe([market])
    row = df.row(0, named=True)

    assert event is not None
    assert event["slug"] == "2026-fifa-world-cup-winner"
    assert row["id"] == "pm-wc-arg-win"
    assert row["event_id"] == "evt-wc-winner"
    assert row["event_slug"] == "2026-fifa-world-cup-winner"
    assert row["clobTokenIds_str"] == '["pm-wc-arg-yes", "pm-wc-arg-no"]'


def test_polymarket_clob_minute_history_replay_contract():
    client = APIClient("https://clob.polymarket.com", retries=0)

    with _replay_vcr().use_cassette("polymarket_clob_minute_history.yml"):
        history = fetch_token_history(
            client,
            "pm-wc-match-home",
            start_ts=1_782_907_200,
            end_ts=1_782_907_320,
            fidelity=1,
        )

    assert history == [
        ("pm-wc-match-home", 1_782_907_230, 0.42),
        ("pm-wc-match-home", 1_782_907_290, 0.57),
    ]


def test_kalshi_events_markets_and_candlesticks_replay_contract():
    client = APIClient("https://api.elections.kalshi.com/trade-api/v2", retries=0)
    scraped_at = datetime(2099, 1, 1, 10, 0, 0)

    with _replay_vcr().use_cassette("kalshi_events_markets_candlesticks.yml"):
        events = fetch_events_for_series(client, "KXMENWORLDCUP")
        markets = fetch_markets_for_event(client, "KXWCSTAGEOFELIM-26ARG")
        candles = fetch_market_candlesticks(
            client,
            series_ticker="KXMENWORLDCUP",
            market_ticker="KXWCSTAGEOFELIM-26ARG-R16",
            start_ts=4_070_941_200,
            end_ts=4_070_948_400,
        )

    event_row = normalize_event_row(events[0], scraped_at=scraped_at)
    market_row = normalize_market_row(markets[0], scraped_at=scraped_at)
    candle_rows = normalize_candlestick_rows(
        "KXWCSTAGEOFELIM-26ARG-R16",
        candles,
        refreshed_at=scraped_at,
    )

    assert event_row["event_ticker"] == "KXWCSTAGEOFELIM-26ARG"
    assert event_row["series_ticker"] == "KXMENWORLDCUP"
    assert market_row["market_ticker"] == "KXWCSTAGEOFELIM-26ARG-R16"
    assert market_row["series_ticker"] == "KXWCSTAGEOFELIM"
    assert candle_rows == [
        {
            "market_ticker": "KXWCSTAGEOFELIM-26ARG-R16",
            "hour_start_utc": scraped_at,
            "open_price": 0.61,
            "high_price": 0.68,
            "low_price": 0.6,
            "close_price": 0.67,
            "avg_price": 0.64,
            "volume": 12,
            "refreshed_at": scraped_at,
        }
    ]
