"""Normalize Kalshi event/market API payloads for warehouse landing."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _parse_ts(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(
            tzinfo=None
        )
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def normalize_event_row(
    event: dict[str, Any], *, scraped_at: datetime | None = None
) -> dict[str, Any]:
    scraped = scraped_at or _utc_now()
    return {
        "event_ticker": str(event.get("event_ticker") or ""),
        "series_ticker": str(event.get("series_ticker") or ""),
        "title": event.get("title"),
        "sub_title": event.get("sub_title"),
        "category": event.get("category"),
        "status": event.get("status"),
        "open_time": _parse_ts(event.get("open_time")),
        "close_time": _parse_ts(event.get("close_time")),
        "scraped_at": scraped,
    }


def normalize_market_row(
    market: dict[str, Any],
    *,
    scraped_at: datetime | None = None,
) -> dict[str, Any]:
    scraped = scraped_at or _utc_now()
    return {
        "market_ticker": str(market.get("ticker") or ""),
        "event_ticker": str(market.get("event_ticker") or ""),
        "series_ticker": _series_ticker_from_market(market),
        "title": market.get("title"),
        "subtitle": market.get("subtitle"),
        "yes_sub_title": market.get("yes_sub_title"),
        "no_sub_title": market.get("no_sub_title"),
        "status": market.get("status"),
        "market_type": market.get("market_type"),
        "open_time": _parse_ts(market.get("open_time")),
        "close_time": _parse_ts(market.get("close_time")),
        "expiration_time": _parse_ts(market.get("expiration_time")),
        "occurrence_datetime": _parse_ts(market.get("occurrence_datetime")),
        "volume": market.get("volume"),
        "open_interest": market.get("open_interest"),
        "last_price_dollars": market.get("last_price_dollars"),
        "scraped_at": scraped,
    }


def _series_ticker_from_market(market: dict[str, Any]) -> str:
    event_ticker = str(market.get("event_ticker") or "")
    if event_ticker and "-" in event_ticker:
        return event_ticker.split("-", 1)[0]
    ticker = str(market.get("ticker") or "")
    if ticker and "-" in ticker:
        return ticker.split("-", 1)[0]
    return ""


def normalize_candlestick_rows(
    market_ticker: str,
    candlesticks: list[dict[str, Any]],
    *,
    refreshed_at: datetime | None = None,
) -> list[dict[str, Any]]:
    refreshed = refreshed_at or _utc_now()
    rows: list[dict[str, Any]] = []
    for candle in candlesticks:
        end_ts = candle.get("end_period_ts")
        if end_ts is None:
            continue
        hour_start = datetime.fromtimestamp(int(end_ts), tz=timezone.utc).replace(
            tzinfo=None
        )
        price = candle.get("price") if isinstance(candle.get("price"), dict) else {}
        rows.append(
            {
                "market_ticker": market_ticker,
                "hour_start_utc": hour_start,
                "open_price": _price_field(price, "open_dollars"),
                "high_price": _price_field(price, "high_dollars"),
                "low_price": _price_field(price, "low_dollars"),
                "close_price": _price_field(price, "close_dollars"),
                "avg_price": _price_field(price, "mean_dollars"),
                "volume": candle.get("volume"),
                "refreshed_at": refreshed,
            }
        )
    return rows


def _price_field(price: dict[str, Any], key: str) -> float | None:
    raw = price.get(key)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


__all__ = [
    "normalize_candlestick_rows",
    "normalize_event_row",
    "normalize_market_row",
]
