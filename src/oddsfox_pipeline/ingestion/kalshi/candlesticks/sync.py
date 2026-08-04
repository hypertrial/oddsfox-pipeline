"""Kalshi hourly candlestick sync."""

from __future__ import annotations

import logging
from dataclasses import dataclass
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
from oddsfox_pipeline.ingestion.kalshi.concurrent import map_bounded
from oddsfox_pipeline.storage.duckdb.kalshi_candlesticks import (
    get_registry_markets_for_sync,
    save_candlesticks_batch,
    upsert_candlestick_ledger_states_batch,
)
from oddsfox_pipeline.storage.duckdb.metadata import save_sync_run_metrics

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MarketFetchResult:
    market_ticker: str
    candlesticks: list[dict[str, Any]]
    empty_run: bool
    last_sync_hour_start: int | None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _trailing_window_hours(*, window_hours: int, history_backfill_days: int) -> int:
    hours = int(window_hours)
    days = int(history_backfill_days)
    if days > 0:
        return min(hours, days * 24)
    return hours


def _fetch_market_candlesticks(
    client: object,
    market: dict[str, Any],
    *,
    start_at: datetime,
    end_at: datetime,
) -> _MarketFetchResult:
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
    return _MarketFetchResult(
        market_ticker=market_ticker,
        candlesticks=candlesticks,
        empty_run=candlesticks == [],
        last_sync_hour_start=int(effective_start.timestamp()),
    )


def sync_hourly_candlesticks(
    *,
    scope_name: str = DEFAULT_KALSHI_WC2026_MARKET_SCOPE,
    window_hours: int = KALSHI_WC2026_HOURLY_WINDOW_HOURS,
    history_backfill_days: int = 0,
    routine_interval_hours: int = 1,
    force: bool = False,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    client_factory: Callable[[], object] | None = None,
) -> dict[str, Any]:
    factory = client_factory or build_client
    client = factory()
    end_at = _utc_now()
    trailing_hours = _trailing_window_hours(
        window_hours=window_hours,
        history_backfill_days=history_backfill_days,
    )
    start_at = end_at - timedelta(hours=trailing_hours)
    markets = get_registry_markets_for_sync(scope_name=scope_name, force=force)
    rows_written = 0
    markets_synced = 0
    empty_markets = 0

    results = map_bounded(
        markets,
        lambda market: _fetch_market_candlesticks(
            client,
            market,
            start_at=start_at,
            end_at=end_at,
        ),
    )
    all_candlesticks: list[dict[str, Any]] = []
    ledger_states: list[tuple[str, bool, bool, int | None]] = []
    for result in results:
        all_candlesticks.extend(result.candlesticks)
        ledger_states.append(
            (
                result.market_ticker,
                True,
                result.empty_run,
                result.last_sync_hour_start,
            )
        )
        markets_synced += 0 if result.empty_run else 1
        empty_markets += 1 if result.empty_run else 0
        rows_written += len(result.candlesticks)
        if progress_callback:
            progress_callback(
                "kalshi_candlesticks",
                {
                    "markets_synced": markets_synced,
                    "rows_written": rows_written,
                    "market_ticker": result.market_ticker,
                },
            )

    rows_written = save_candlesticks_batch(all_candlesticks)
    upsert_candlestick_ledger_states_batch(
        ledger_states,
        routine_interval_hours=int(routine_interval_hours),
        scope_name=scope_name,
    )

    metrics = {
        "scope_name": scope_name,
        "window_hours": window_hours,
        "history_backfill_days": history_backfill_days,
        "routine_interval_hours": routine_interval_hours,
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
