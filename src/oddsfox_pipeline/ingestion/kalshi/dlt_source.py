"""dlt resources for Kalshi raw event/market landing."""

from __future__ import annotations

from typing import Any, Iterable

import dlt

from oddsfox_pipeline.ingestion.kalshi.markets.sync import collect_market_scope_payload
from oddsfox_pipeline.naming import KALSHI_WC2026
from oddsfox_pipeline.storage.duckdb.dlt_batch import DLT_STRICT_SCHEMA_CONTRACT

_EVENT_COLUMNS = {
    "event_ticker": {"data_type": "text"},
    "series_ticker": {"data_type": "text"},
    "title": {"data_type": "text", "nullable": True},
    "sub_title": {"data_type": "text", "nullable": True},
    "category": {"data_type": "text", "nullable": True},
    "status": {"data_type": "text", "nullable": True},
    "open_time": {"data_type": "timestamp", "timezone": False, "nullable": True},
    "close_time": {"data_type": "timestamp", "timezone": False, "nullable": True},
    "scraped_at": {"data_type": "timestamp", "timezone": False},
}

_MARKET_COLUMNS = {
    "market_ticker": {"data_type": "text"},
    "event_ticker": {"data_type": "text"},
    "series_ticker": {"data_type": "text"},
    "title": {"data_type": "text", "nullable": True},
    "subtitle": {"data_type": "text", "nullable": True},
    "yes_sub_title": {"data_type": "text", "nullable": True},
    "no_sub_title": {"data_type": "text", "nullable": True},
    "status": {"data_type": "text", "nullable": True},
    "market_type": {"data_type": "text", "nullable": True},
    "open_time": {"data_type": "timestamp", "timezone": False, "nullable": True},
    "close_time": {"data_type": "timestamp", "timezone": False, "nullable": True},
    "expiration_time": {"data_type": "timestamp", "timezone": False, "nullable": True},
    "volume": {"data_type": "bigint", "nullable": True},
    "open_interest": {"data_type": "bigint", "nullable": True},
    "last_price_dollars": {"data_type": "text", "nullable": True},
    "scraped_at": {"data_type": "timestamp", "timezone": False},
}


def collect_raw_events_and_markets(
    *,
    scope_name: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    payload = collect_market_scope_payload(scope_name=scope_name)
    return {
        "events": payload["events"],
        "markets": payload["markets"],
    }


def kalshi_wc2026_source(
    events: Iterable[dict[str, Any]] = (),
    markets: Iterable[dict[str, Any]] = (),
    *,
    source_name: str = KALSHI_WC2026,
):
    @dlt.source(name=source_name)
    def _source():
        @dlt.resource(
            name="events",
            primary_key="event_ticker",
            write_disposition="merge",
            schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
            columns=_EVENT_COLUMNS,
        )
        def events_resource():
            yield from events

        @dlt.resource(
            name="markets",
            primary_key="market_ticker",
            write_disposition="merge",
            schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
            columns=_MARKET_COLUMNS,
        )
        def markets_resource():
            yield from markets

        return events_resource, markets_resource

    return _source()


__all__ = [
    "collect_raw_events_and_markets",
    "kalshi_wc2026_source",
]
