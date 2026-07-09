"""Kalshi public API HTTP client."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Iterator

from oddsfox_pipeline.config.settings import KALSHI_API_URL, KALSHI_REQUESTS_PER_SECOND
from oddsfox_pipeline.resources.http import APIClient, RateLimiter
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_LIMIT = 200
_MAX_429_RETRIES = 5


def build_client(requests_per_second: int | None = None) -> APIClient:
    rps = (
        KALSHI_REQUESTS_PER_SECOND
        if requests_per_second is None
        else requests_per_second
    )
    limiter = RateLimiter(float(rps)) if rps and rps > 0 else None
    validate_outbound_https_url(KALSHI_API_URL)
    return APIClient(base_url=KALSHI_API_URL, rate_limiter=limiter)


def _get_with_429_backoff(
    client: APIClient,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    attempt = 0
    while True:
        try:
            return client.get(endpoint, params=params)
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status != 429 or attempt >= _MAX_429_RETRIES:
                raise
            sleep_s = min(30.0, 2.0**attempt)
            logger.warning("Kalshi 429 on %s; backing off %.1fs", endpoint, sleep_s)
            time.sleep(sleep_s)
            attempt += 1


def paginate(
    client: APIClient,
    endpoint: str,
    *,
    collection_key: str,
    params: dict[str, Any] | None = None,
    limit: int = _DEFAULT_PAGE_LIMIT,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> Iterator[dict[str, Any]]:
    query = dict(params or {})
    query.setdefault("limit", limit)
    cursor: str | None = None
    pages = 0
    while True:
        if cursor:
            query["cursor"] = cursor
        elif "cursor" in query:
            query.pop("cursor", None)
        payload = _get_with_429_backoff(client, endpoint, params=query)
        rows = payload.get(collection_key) or []
        pages += 1
        if progress_callback:
            progress_callback(
                "kalshi_page",
                {"endpoint": endpoint, "pages": pages, "rows": len(rows)},
            )
        for row in rows:
            if isinstance(row, dict):
                yield row
        cursor = payload.get("cursor")
        if not cursor:
            break


def fetch_events_for_series(
    client: APIClient,
    series_ticker: str,
    *,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    return list(
        paginate(
            client,
            "/events",
            collection_key="events",
            params={"series_ticker": series_ticker},
            progress_callback=progress_callback,
        )
    )


def fetch_markets_for_event(
    client: APIClient,
    event_ticker: str,
    *,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    return list(
        paginate(
            client,
            "/markets",
            collection_key="markets",
            params={"event_ticker": event_ticker},
            progress_callback=progress_callback,
        )
    )


def fetch_market_candlesticks(
    client: APIClient,
    *,
    series_ticker: str,
    market_ticker: str,
    start_ts: int,
    end_ts: int,
    period_interval: int = 60,
) -> list[dict[str, Any]]:
    # ponytail: no /historical/* fallback until GET /historical/cutoff advances past WC2026.
    payload = _get_with_429_backoff(
        client,
        f"/series/{series_ticker}/markets/{market_ticker}/candlesticks",
        params={
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval,
        },
    )
    candlesticks = payload.get("candlesticks") or []
    return [row for row in candlesticks if isinstance(row, dict)]


__all__ = [
    "build_client",
    "fetch_events_for_series",
    "fetch_market_candlesticks",
    "fetch_markets_for_event",
    "paginate",
]
