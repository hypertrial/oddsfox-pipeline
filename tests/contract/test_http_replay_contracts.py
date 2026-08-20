from __future__ import annotations

from datetime import datetime, timezone
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
from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    PMXT_ORDER_BOOK_ENDPOINT,
    build_pmxt_client,
    load_order_book_manifest,
    normalize_pmxt_snapshot,
)
from oddsfox_pipeline.ingestion.polymarket.match_trades import (
    ENDPOINT as PMXT_TRADES_ENDPOINT,
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
    client = APIClient(
        "https://gamma-api.polymarket.com", source_id="polymarket", retries=0
    )

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
    client = APIClient("https://clob.polymarket.com", source_id="polymarket", retries=0)

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


def test_pmxt_order_book_payload_replay_contract():
    manifest = load_order_book_manifest()
    target = manifest.targets[0]
    outcome = target.outcomes[0]
    client = build_pmxt_client(requests_per_minute=60)

    with _replay_vcr().use_cassette("pmxt_order_book.yml"):
        payload = client.post(
            PMXT_ORDER_BOOK_ENDPOINT,
            headers={"Authorization": "Bearer synthetic-replay-key"},
            json={
                "args": [
                    target.condition_id,
                    None,
                    {
                        "since": target.window_start_ms,
                        "until": target.window_end_ms,
                        "outcome": outcome.clob_token_id,
                        "limit": 1_000,
                    },
                ]
            },
        )

    assert payload["success"] is True
    row = normalize_pmxt_snapshot(
        payload["data"][0],
        manifest=manifest,
        target=target,
        outcome=outcome,
        scan_id="replay",
        window_start_ms=target.window_start_ms,
        window_end_ms=target.window_end_ms,
    )
    assert row["bids_json"] == ('[{"order_count":2,"price":"0.4","size":"10"}]')
    assert row["asks_json"] == ('[{"order_count":1,"price":"0.6","size":"5"}]')


def test_pmxt_trades_payload_replay_contract():
    manifest = load_order_book_manifest()
    target = manifest.targets[0]
    outcome = target.outcomes[0]
    client = build_pmxt_client(requests_per_minute=60)
    start_iso = (
        datetime.fromtimestamp(target.window_start_ms / 1_000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
    end_iso = (
        datetime.fromtimestamp(target.window_end_ms / 1_000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    with _replay_vcr().use_cassette("pmxt_trades.yml"):
        payload = client.get(
            PMXT_TRADES_ENDPOINT,
            headers={"Authorization": "Bearer synthetic-replay-key"},
            params={
                "outcomeId": outcome.clob_token_id,
                "start": start_iso,
                "end": end_iso,
                "limit": 1_000,
            },
        )

    assert payload["success"] is True
    trade = payload["data"][0]
    assert trade["id"] == "pmxt-trade-home-1"
    assert trade["outcomeId"] == outcome.clob_token_id


def test_kalshi_events_markets_and_candlesticks_replay_contract():
    client = APIClient(
        "https://api.elections.kalshi.com/trade-api/v2",
        source_id="kalshi",
        retries=0,
    )
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
    assert market_row["volume"] == 120
    assert market_row["open_interest"] == 42
    assert candle_rows == [
        {
            "market_ticker": "KXWCSTAGEOFELIM-26ARG-R16",
            "hour_start_utc": datetime.fromtimestamp(
                4_070_941_200, tz=timezone.utc
            ).replace(tzinfo=None),
            "open_price": 0.61,
            "high_price": 0.68,
            "low_price": 0.6,
            "close_price": 0.67,
            "avg_price": 0.64,
            "volume": 12,
            "refreshed_at": scraped_at,
        }
    ]
