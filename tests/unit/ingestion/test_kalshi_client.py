"""Unit tests for Kalshi public API client pagination and backoff."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oddsfox_pipeline.ingestion.kalshi import client as kalshi_client


class _Fake429(Exception):
    def __init__(self, status_code: int) -> None:
        self.response = MagicMock(status_code=status_code)


def test_build_client_uses_kalshi_api_url_and_rate_limiter():
    c = kalshi_client.build_client(requests_per_second=3)
    assert c.base_url.endswith("/trade-api/v2")
    assert c.rate_limiter is not None
    assert c.rate_limiter.get_rate() == 3.0


def test_build_client_allows_unlimited_when_rps_zero():
    c = kalshi_client.build_client(requests_per_second=0)
    assert c.rate_limiter is None


def test_paginate_yields_rows_across_cursor_pages():
    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"event_ticker": "KXWC-EVT1"}, "not-a-dict"],
            "cursor": "page-2",
        },
        {"events": [{"event_ticker": "KXWC-EVT2"}], "cursor": None},
    ]
    progress = []

    rows = list(
        kalshi_client.paginate(
            client,
            "/events",
            collection_key="events",
            params={"series_ticker": "KXMENWORLDCUP"},
            progress_callback=lambda phase, payload: progress.append((phase, payload)),
        )
    )

    assert [row["event_ticker"] for row in rows] == ["KXWC-EVT1", "KXWC-EVT2"]
    assert client.get.call_count == 2
    assert client.get.call_args_list[0].kwargs["params"]["limit"] == 200
    assert client.get.call_args_list[1].kwargs["params"]["cursor"] == "page-2"
    assert progress[0][0] == "kalshi_page"
    assert progress[0][1]["pages"] == 1
    assert progress[1][1]["pages"] == 2


def test_fetch_events_for_series_and_markets_for_event_delegate_to_paginate(
    monkeypatch,
):
    client = MagicMock()

    def fake_paginate(_client, endpoint, *, collection_key, params=None, **kwargs):
        del kwargs
        assert _client is client
        if endpoint == "/events":
            assert collection_key == "events"
            assert params == {"series_ticker": "KXMENWORLDCUP"}
            yield {"event_ticker": "KXWC-EVT1"}
        elif endpoint == "/markets":
            assert collection_key == "markets"
            assert params == {"event_ticker": "KXWC-EVT1"}
            yield {"ticker": "KXWC-MKT1"}
        else:
            raise AssertionError(endpoint)

    monkeypatch.setattr(kalshi_client, "paginate", fake_paginate)

    events = kalshi_client.fetch_events_for_series(client, "KXMENWORLDCUP")
    markets = kalshi_client.fetch_markets_for_event(client, "KXWC-EVT1")

    assert events == [{"event_ticker": "KXWC-EVT1"}]
    assert markets == [{"ticker": "KXWC-MKT1"}]


def test_fetch_market_candlesticks_filters_non_dict_rows():
    client = MagicMock()
    client.get.return_value = {
        "candlesticks": [
            {"end_period_ts": 1_700_000_000, "price": {"open_dollars": "0.5"}},
            "skip-me",
        ]
    }

    rows = kalshi_client.fetch_market_candlesticks(
        client,
        series_ticker="KXMENWORLDCUP",
        market_ticker="KXWC-MKT1",
        start_ts=1_699_000_000,
        end_ts=1_700_100_000,
    )

    assert len(rows) == 1
    client.get.assert_called_once_with(
        "/series/KXMENWORLDCUP/markets/KXWC-MKT1/candlesticks",
        params={
            "start_ts": 1_699_000_000,
            "end_ts": 1_700_100_000,
            "period_interval": 60,
        },
    )


def test_get_with_429_backoff_retries_then_raises(no_sleep):
    client = MagicMock()
    client.get.side_effect = [
        _Fake429(429),
        _Fake429(429),
        {"events": [], "cursor": None},
    ]

    payload = kalshi_client._get_with_429_backoff(client, "/events")

    assert payload == {"events": [], "cursor": None}
    assert client.get.call_count == 3


def test_get_with_429_backoff_reraises_after_max_retries(no_sleep):
    client = MagicMock()
    client.get.side_effect = _Fake429(429)

    with pytest.raises(_Fake429):
        kalshi_client._get_with_429_backoff(client, "/events")

    assert client.get.call_count == kalshi_client._MAX_429_RETRIES + 1


def test_paginate_pops_stale_cursor_and_skips_progress_callback():
    client = MagicMock()
    client.get.return_value = {
        "events": [{"event_ticker": "KXWC-EVT1"}],
        "cursor": None,
    }

    rows = list(
        kalshi_client.paginate(
            client,
            "/events",
            collection_key="events",
            params={"series_ticker": "KXWC", "cursor": "stale"},
        )
    )

    assert [row["event_ticker"] for row in rows] == ["KXWC-EVT1"]
    assert "cursor" not in client.get.call_args.kwargs["params"]
