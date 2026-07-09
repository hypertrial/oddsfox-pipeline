"""Kalshi hourly candlestick sync."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from oddsfox_pipeline.config.settings import (
    DEFAULT_KALSHI_WC2026_MARKET_SCOPE,
    KALSHI_WC2026_HOURLY_WINDOW_HOURS,
)
from oddsfox_pipeline.ingestion.kalshi.candlesticks.fetch import (
    fetch_hourly_candlesticks,
)
from oddsfox_pipeline.ingestion.kalshi.client import build_client
from oddsfox_pipeline.storage.duckdb.kalshi_candlesticks import (
    get_registry_markets_for_sync,
    save_candlesticks_batch,
    upsert_candlestick_ledger_state,
)
from oddsfox_pipeline.storage.duckdb.metadata import save_sync_run_metrics

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sync_hourly_candlesticks(
    *,
    scope_name: str = DEFAULT_KALSHI_WC2026_MARKET_SCOPE,
    window_hours: int = KALSHI_WC2026_HOURLY_WINDOW_HOURS,
    force: bool = False,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    client_factory: Callable[[], object] | None = None,
) -> dict[str, Any]:
    factory = client_factory or build_client
    client = factory()
    end_at = _utc_now()
    start_at = end_at - timedelta(hours=int(window_hours))
    markets = get_registry_markets_for_sync(scope_name=scope_name, force=force)
    rows_written = 0
    markets_synced = 0
    empty_markets = 0

    for market in markets:
        market_ticker = market["market_ticker"]
        series_ticker = market["series_ticker"]
        open_time = market.get("open_time")
        effective_start = start_at
        if isinstance(open_time, datetime):
            if open_time.tzinfo is None:
                open_time = open_time.replace(tzinfo=timezone.utc)
            effective_start = max(start_at, open_time.astimezone(timezone.utc))
        candlesticks = fetch_hourly_candlesticks(
            client,
            series_ticker=series_ticker,
            market_ticker=market_ticker,
            start_at=effective_start,
            end_at=end_at,
        )
        if candlesticks:
            rows_written += save_candlesticks_batch(candlesticks)
            markets_synced += 1
        else:
            empty_markets += 1
        upsert_candlestick_ledger_state(
            market_ticker=market_ticker,
            fully_checked=True,
            empty_run=candlesticks == [],
        )
        if progress_callback:
            progress_callback(
                "kalshi_candlesticks",
                {
                    "markets_synced": markets_synced,
                    "rows_written": rows_written,
                    "market_ticker": market_ticker,
                },
            )

    metrics = {
        "scope_name": scope_name,
        "window_hours": window_hours,
        "markets_total": len(markets),
        "markets_synced": markets_synced,
        "empty_markets": empty_markets,
        "rows_written": rows_written,
        "force": force,
    }
    save_sync_run_metrics(
        "sync_kalshi_candlesticks",
        metrics,
        scope_name=scope_name,
        source="kalshi",
    )
    logger.info("Kalshi candlestick sync complete: %s", metrics)
    return metrics


__all__ = ["sync_hourly_candlesticks"]
