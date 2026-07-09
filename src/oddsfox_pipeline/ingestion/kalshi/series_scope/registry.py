"""Kalshi series-scope registry refresh."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from oddsfox_pipeline.ingestion.kalshi.client import (
    fetch_events_for_series,
    fetch_markets_for_event,
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


def _series_ticker_from_market(market: dict[str, Any]) -> str:
    ticker = str(market.get("ticker") or "")
    event_ticker = str(market.get("event_ticker") or "")
    if event_ticker and "-" in event_ticker:
        return event_ticker.split("-", 1)[0]
    if ticker and "-" in ticker:
        return ticker.split("-", 1)[0]
    return ""


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

    for series_ticker in cfg.series_tickers:
        series_events = fetch_events_for_series(
            client,
            series_ticker,
            progress_callback=progress_callback,
        )
        api_requests += max(1, len(series_events) // 200 + 1)
        for event in series_events:
            event_ticker = str(event.get("event_ticker") or "")
            if not event_ticker:
                continue
            events.append(event)
            event_markets = fetch_markets_for_event(
                client,
                event_ticker,
                progress_callback=progress_callback,
            )
            api_requests += max(1, len(event_markets) // 200 + 1)
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

    upserted = upsert_registry_rows(registry_rows)
    elapsed = time.monotonic() - t0
    summary = {
        "scope_name": cfg.scope_name,
        "series_tickers": list(cfg.series_tickers),
        "events_collected": len(events),
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
