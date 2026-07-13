"""Unit tests for Kalshi event/market/candlestick normalization."""

from __future__ import annotations

from datetime import datetime, timezone

from oddsfox_pipeline.ingestion.kalshi.markets import transform


def test_normalize_event_row_parses_timestamps_and_defaults():
    scraped = datetime(2026, 1, 1, 12, 0, 0)
    row = transform.normalize_event_row(
        {
            "event_ticker": "KXMENWORLDCUP-WINNER",
            "series_ticker": "KXMENWORLDCUP",
            "title": "World Cup winner",
            "open_time": "2025-01-01T00:00:00Z",
            "close_time": 1_735_689_600,
        },
        scraped_at=scraped,
    )

    assert row["event_ticker"] == "KXMENWORLDCUP-WINNER"
    assert row["series_ticker"] == "KXMENWORLDCUP"
    assert row["open_time"] == datetime(2025, 1, 1, 0, 0, 0)
    assert row["close_time"] == datetime.fromtimestamp(
        1_735_689_600, tz=timezone.utc
    ).replace(tzinfo=None)
    assert row["scraped_at"] == scraped


def test_normalize_market_row_derives_series_ticker_from_event_ticker():
    row = transform.normalize_market_row(
        {
            "ticker": "KXMENWORLDCUP-WINNER-USA",
            "event_ticker": "KXMENWORLDCUP-WINNER",
            "title": "USA to win",
            "last_price_dollars": "0.12",
            "volume": 1000,
            "occurrence_datetime": "2026-07-14T19:00:00Z",
        },
        scraped_at=datetime(2026, 1, 1),
    )

    assert row["market_ticker"] == "KXMENWORLDCUP-WINNER-USA"
    assert row["series_ticker"] == "KXMENWORLDCUP"
    assert row["last_price_dollars"] == "0.12"
    assert row["volume"] == 1000
    assert row["occurrence_datetime"] == datetime(2026, 7, 14, 19)


def test_normalize_candlestick_rows_skips_missing_end_period_ts():
    refreshed = datetime(2026, 1, 1, 0, 0, 0)
    rows = transform.normalize_candlestick_rows(
        "KXMENWORLDCUP-WINNER-USA",
        [
            {
                "end_period_ts": 1_700_000_000,
                "price": {"close_dollars": "0.4"},
                "volume": 5,
            },
            {"price": {"close_dollars": "0.1"}},
        ],
        refreshed_at=refreshed,
    )

    assert len(rows) == 1
    assert rows[0]["market_ticker"] == "KXMENWORLDCUP-WINNER-USA"
    assert rows[0]["close_price"] == 0.4
    assert rows[0]["volume"] == 5
    assert rows[0]["refreshed_at"] == refreshed
    assert rows[0]["hour_start_utc"] == datetime.fromtimestamp(
        1_700_000_000, tz=timezone.utc
    ).replace(tzinfo=None)


def test_parse_ts_handles_datetime_numeric_and_invalid_values():
    aware = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert transform.normalize_event_row(
        {"event_ticker": "e", "open_time": aware},
        scraped_at=datetime(2026, 1, 1),
    )["open_time"] == datetime(2025, 6, 1, 12, 0, 0)
    assert (
        transform.normalize_event_row(
            {"event_ticker": "e", "open_time": "   "},
            scraped_at=datetime(2026, 1, 1),
        )["open_time"]
        is None
    )
    assert (
        transform.normalize_event_row(
            {"event_ticker": "e", "open_time": "not-a-date"},
            scraped_at=datetime(2026, 1, 1),
        )["open_time"]
        is None
    )


def test_normalize_market_row_uses_default_scraped_at_and_ticker_series():
    row = transform.normalize_market_row(
        {"ticker": "SERIES-MKT1", "event_ticker": ""},
    )
    assert row["series_ticker"] == "SERIES"
    assert row["scraped_at"] is not None


def test_normalize_market_row_returns_empty_series_when_no_dash():
    row = transform.normalize_market_row(
        {"ticker": "NODASH", "event_ticker": ""},
        scraped_at=datetime(2026, 1, 1),
    )
    assert row["series_ticker"] == ""


def test_normalize_candlestick_rows_parses_all_price_fields_and_skips_bad_values():
    rows = transform.normalize_candlestick_rows(
        "KXWC-MKT1",
        [
            {
                "end_period_ts": 1_700_000_000,
                "price": {
                    "open_dollars": "0.1",
                    "high_dollars": "0.2",
                    "low_dollars": "0.05",
                    "close_dollars": "0.15",
                    "mean_dollars": "0.12",
                },
                "volume": 9,
            },
            {"end_period_ts": 1_700_000_060, "price": "not-a-dict"},
            {
                "end_period_ts": 1_700_000_120,
                "price": {"close_dollars": "bad"},
            },
        ],
        refreshed_at=datetime(2026, 1, 1),
    )

    assert len(rows) == 3
    assert rows[0]["open_price"] == 0.1
    assert rows[0]["avg_price"] == 0.12
    assert rows[2]["close_price"] is None
