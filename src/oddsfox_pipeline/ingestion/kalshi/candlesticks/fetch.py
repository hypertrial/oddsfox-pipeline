"""Kalshi hourly candlestick fetch helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oddsfox_pipeline.ingestion.kalshi.client import fetch_market_candlesticks
from oddsfox_pipeline.ingestion.kalshi.markets.transform import (
    normalize_candlestick_rows,
)


def _to_epoch_seconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def fetch_hourly_candlesticks(
    client: object,
    *,
    series_ticker: str,
    market_ticker: str,
    start_at: datetime,
    end_at: datetime,
) -> list[dict[str, Any]]:
    raw = fetch_market_candlesticks(
        client,
        series_ticker=series_ticker,
        market_ticker=market_ticker,
        start_ts=_to_epoch_seconds(start_at),
        end_ts=_to_epoch_seconds(end_at),
        period_interval=60,
    )
    return normalize_candlestick_rows(market_ticker, raw)


__all__ = ["fetch_hourly_candlesticks"]
