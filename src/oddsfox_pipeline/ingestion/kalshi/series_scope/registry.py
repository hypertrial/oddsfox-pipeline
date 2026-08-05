"""Kalshi series-scope registry refresh."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from oddsfox_pipeline.ingestion.kalshi.client import (
    fetch_events_for_series,
    fetch_markets_for_event,
)
from oddsfox_pipeline.ingestion.kalshi.concurrent import map_bounded
from oddsfox_pipeline.ingestion.kalshi.markets.transform import (
    _series_ticker_from_market,
)
from oddsfox_pipeline.ingestion.kalshi.series_scope.config import (
    KalshiMarketScopeConfig,
    load_market_scope_config,
    market_suffix_excluded,
)
from oddsfox_pipeline.storage.duckdb.kalshi_market_scope_registry import (
    KalshiRegistryRow,
    upsert_registry_rows,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KalshiCollectResult:
    events: list[dict[str, Any]]
    markets: list[dict[str, Any]]
    registry_rows: list[KalshiRegistryRow]
    summary: dict[str, Any]


@dataclass(frozen=True)
class _EventCollection:
    event: dict[str, Any]
    markets: list[dict[str, Any]]
    registry_rows: list[KalshiRegistryRow]
    api_requests: int


def _collect_event_markets(
    client: object,
    event: dict[str, Any],
    *,
    series_ticker: str,
    cfg: KalshiMarketScopeConfig,
    progress_callback: Callable[[str, dict[str, Any]], None] | None,
) -> _EventCollection | None:
    event_ticker = str(event.get("event_ticker") or "")
    if not event_ticker:
        return None
    event_markets = fetch_markets_for_event(
        client,
        event_ticker,
        progress_callback=progress_callback,
    )
    api_requests = max(1, len(event_markets) // 200 + 1)
    markets: list[dict[str, Any]] = []
    registry_rows: list[KalshiRegistryRow] = []
    for market in event_markets:
        market_ticker = str(market.get("ticker") or "")
        if not market_ticker:
            continue
        series = _series_ticker_from_market(market) or series_ticker
        if market_suffix_excluded(
            cfg,
            series_ticker=series,
            market_ticker=market_ticker,
        ):
            continue
        markets.append(market)
        registry_rows.append(
            KalshiRegistryRow(
                scope_name=cfg.scope_name,
                market_ticker=market_ticker,
                event_ticker=event_ticker,
                series_ticker=series,
                source="series_api",
            )
        )
    return _EventCollection(
        event=event,
        markets=markets,
        registry_rows=registry_rows,
        api_requests=api_requests,
    )


def refresh_registry_and_collect(
    client: object,
    *,
    config: KalshiMarketScopeConfig | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> KalshiCollectResult:
    cfg = config or load_market_scope_config()
    t0 = time.monotonic()
    events: list[dict[str, Any]] = []
    markets: list[dict[str, Any]] = []
    registry_rows: list[KalshiRegistryRow] = []
    api_requests = 0
    events_failed = 0
    failed_lock = threading.Lock()

    def _on_event_error(_event: dict[str, Any], _exc: Exception) -> None:
        nonlocal events_failed
        with failed_lock:
            events_failed += 1

    for series_ticker in cfg.series_tickers:
        series_events = fetch_events_for_series(
            client,
            series_ticker,
            progress_callback=progress_callback,
        )
        api_requests += max(1, len(series_events) // 200 + 1)
        collected = map_bounded(
            series_events,
            lambda event: _collect_event_markets(
                client,
                event,
                series_ticker=series_ticker,
                cfg=cfg,
                progress_callback=progress_callback,
            ),
            on_error=_on_event_error,
        )
        for item in collected:
            if item is None:  # pragma: no cover - map_bounded filters None results
                continue
            events.append(item.event)
            markets.extend(item.markets)
            registry_rows.extend(item.registry_rows)
            api_requests += item.api_requests

    upserted = upsert_registry_rows(registry_rows)
    elapsed = time.monotonic() - t0
    summary = {
        "scope_name": cfg.scope_name,
        "series_tickers": list(cfg.series_tickers),
        "events_collected": len(events),
        "events_failed": events_failed,
        "markets_collected": len(markets),
        "registry_rows": len(registry_rows),
        "registry_upserted": upserted,
        "api_requests": api_requests,
        "elapsed_seconds": round(elapsed, 3),
    }
    logger.info("Kalshi registry refresh: %s", summary)
    return KalshiCollectResult(
        events=events,
        markets=markets,
        registry_rows=registry_rows,
        summary=summary,
    )


__all__ = ["KalshiCollectResult", "refresh_registry_and_collect"]
