"""dlt resources for Polymarket raw market landing."""

from __future__ import annotations

from typing import Any, Iterable

import dlt

from oddsfox_pipeline.config.settings import POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    DISCOVERY_MODE_FULL_KEYSET,
    DiscoveryMode,
)
from oddsfox_pipeline.ingestion.polymarket.markets.persistence import (
    market_records_to_dicts,
    prepare_batch_for_db,
)
from oddsfox_pipeline.ingestion.polymarket.markets.sync import (
    collect_market_scope_payload,
)
from oddsfox_pipeline.ingestion.polymarket.markets.transform import (
    process_markets_dataframe,
)
from oddsfox_pipeline.naming import POLYMARKET_US_MIDTERMS_2026, POLYMARKET_WC2026
from oddsfox_pipeline.storage.duckdb.dlt_batch import DLT_STRICT_SCHEMA_CONTRACT

_MARKET_COLUMNS = {
    "id": {"data_type": "text"},
    "question": {"data_type": "text"},
    "category": {"data_type": "text"},
    "description": {"data_type": "text"},
    "outcomes": {"data_type": "text"},
    "volume": {"data_type": "double"},
    "active": {"data_type": "bool"},
    "closed": {"data_type": "bool"},
    "created_at": {"data_type": "timestamp", "timezone": False},
    "scraped_at": {"data_type": "timestamp", "timezone": False},
    "end_date": {"data_type": "timestamp", "timezone": False},
    "slug": {"data_type": "text"},
    "event_slug": {"data_type": "text"},
    "event_id": {"data_type": "text"},
    "condition_id": {"data_type": "text"},
    "sports_market_type": {"data_type": "text"},
    "game_start_time": {"data_type": "timestamp", "timezone": False},
    "group_item_title": {"data_type": "text"},
    "tags": {"data_type": "text"},
    "clob_token_ids": {"data_type": "text"},
    "is_resolved": {"data_type": "bool"},
    "winning_outcome": {"data_type": "text"},
    "winning_clob_token_id": {"data_type": "text"},
}


def collect_raw_markets(
    *,
    discovery_mode: DiscoveryMode = DISCOVERY_MODE_FULL_KEYSET,
    scope_name: str | None = None,
    max_event_pages: int | None = None,
    max_pages_without_progress: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: list[str] | None = None,
    keyset_volume_min: float | None = POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN,
) -> list[dict[str, Any]]:
    collection = collect_market_scope_payload(
        discovery_mode=discovery_mode,
        scope_name=scope_name,
        max_event_pages=max_event_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=keyset_volume_min,
    )
    return list(collection["raw_markets"])


def normalize_market_payloads_for_dlt(
    markets: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    df = process_markets_dataframe(list(markets))
    market_rows, _ = prepare_batch_for_db(df)
    return market_records_to_dicts(market_rows)


def polymarket_markets_source(
    rows: Iterable[dict[str, Any]] = (),
    *,
    source_name: str = POLYMARKET_WC2026,
):
    @dlt.source(name=source_name)
    def _source():
        @dlt.resource(
            name="markets",
            primary_key="id",
            write_disposition="merge",
            schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
            columns=_MARKET_COLUMNS,
        )
        def markets():
            yield from rows

        return markets

    return _source()


def polymarket_wc2026_markets_source(rows: Iterable[dict[str, Any]] = ()):
    return polymarket_markets_source(rows, source_name=POLYMARKET_WC2026)


def polymarket_us_midterms_2026_markets_source(rows: Iterable[dict[str, Any]] = ()):
    return polymarket_markets_source(rows, source_name=POLYMARKET_US_MIDTERMS_2026)


__all__ = [
    "collect_raw_markets",
    "normalize_market_payloads_for_dlt",
    "polymarket_markets_source",
    "polymarket_us_midterms_2026_markets_source",
    "polymarket_wc2026_markets_source",
]
