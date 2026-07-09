"""Orchestration for Kalshi events/markets sync."""

from __future__ import annotations

import logging
from typing import Any, Callable

from oddsfox_pipeline.ingestion.kalshi.client import build_client
from oddsfox_pipeline.ingestion.kalshi.markets.transform import (
    normalize_event_row,
    normalize_market_row,
)
from oddsfox_pipeline.ingestion.kalshi.series_scope.config import (
    load_market_scope_config,
)
from oddsfox_pipeline.ingestion.kalshi.series_scope.registry import (
    refresh_registry_and_collect,
)
from oddsfox_pipeline.storage.duckdb.metadata import save_sync_run_metrics

logger = logging.getLogger(__name__)


def collect_market_scope_payload(
    client_factory: Callable[[], object] | None = None,
    *,
    scope_name: str | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    factory = client_factory or build_client
    client = factory()
    cfg = load_market_scope_config(scope_name=scope_name)
    result = refresh_registry_and_collect(
        client,
        config=cfg,
        progress_callback=progress_callback,
    )
    scraped_at = None
    events = [normalize_event_row(row, scraped_at=scraped_at) for row in result.events]
    markets = [
        normalize_market_row(row, scraped_at=scraped_at) for row in result.markets
    ]
    payload = {
        "scope_name": cfg.scope_name,
        "events": events,
        "markets": markets,
        "registry_summary": result.summary,
        "total_events": len(events),
        "total_markets": len(markets),
    }
    return payload


def sync_markets(
    *,
    scope_name: str | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    client_factory: Callable[[], object] | None = None,
) -> dict[str, Any]:
    payload = collect_market_scope_payload(
        client_factory=client_factory,
        scope_name=scope_name,
        progress_callback=progress_callback,
    )
    metrics = {
        "scope_name": payload["scope_name"],
        "total_events": payload["total_events"],
        "total_markets": payload["total_markets"],
        "registry_summary": payload["registry_summary"],
    }
    save_sync_run_metrics(
        "sync_kalshi_markets",
        metrics,
        scope_name=payload["scope_name"],
        source="kalshi",
    )
    logger.info("Kalshi markets sync complete: %s", metrics)
    return payload


__all__ = ["collect_market_scope_payload", "sync_markets"]
