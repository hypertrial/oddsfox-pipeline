"""
Persistence helpers for raw market ingestion.

This module holds DB-facing batching logic so the sync orchestration stays
focused on control flow rather than storage details.
"""

from datetime import datetime, timezone
from typing import Iterable, List, Tuple

import polars as pl

MARKET_RECORD_COLUMNS = (
    "id",
    "question",
    "category",
    "description",
    "outcomes",
    "volume",
    "active",
    "closed",
    "created_at",
    "scraped_at",
    "end_date",
    "slug",
    "event_slug",
    "event_id",
    "event_title",
    "event_start_time",
    "event_finished_time",
    "event_game_id",
    "event_ended",
    "condition_id",
    "sports_market_type",
    "game_start_time",
    "group_item_title",
    "tags",
    "clob_token_ids",
    "is_resolved",
    "winning_outcome",
    "winning_clob_token_id",
)


def _utc_now() -> datetime:
    """Return the current UTC time in the warehouse's naive timestamp shape."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def market_records_to_dicts(market_data: Iterable[Tuple]) -> list[dict]:
    rows_by_id: dict[str, dict] = {}
    for row in market_data:
        payload = dict(zip(MARKET_RECORD_COLUMNS, row, strict=True))
        rows_by_id[str(payload["id"])] = payload
    return list(rows_by_id.values())


def _format_timestamp(value: object) -> object:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def prepare_batch_for_db(df: pl.DataFrame) -> Tuple[List[Tuple], List[Tuple]]:
    """
    Convert processed DataFrame into lists of tuples for DB insertion.
    Returns: (market_records, token_records)
    """
    if df.is_empty():
        return [], []

    columns = set(df.columns)
    scraped_at = _utc_now().isoformat()

    market_data = []
    token_data = []

    for row in df.to_dicts():
        volume = row.get("volumeNum") if "volumeNum" in columns else row.get("volume")
        active = row.get("active", False)
        closed = row.get("closed", False)
        event_ended = row.get("event_ended")
        is_resolved = row.get("is_resolved")
        market_id = row.get("id", "")

        market_data.append(
            (
                market_id,
                row.get("question", ""),
                row.get("category", ""),
                row.get("description", ""),
                row.get("outcomes_str", ""),
                float(volume) if volume is not None else 0.0,
                bool(active) if active is not None else None,
                bool(closed) if closed is not None else None,
                _format_timestamp(row.get("created_at", "")),
                scraped_at,
                _format_timestamp(row.get("end_date", "")),
                row.get("slug"),
                row.get("event_slug"),
                row.get("event_id"),
                row.get("event_title"),
                row.get("event_start_time"),
                row.get("event_finished_time"),
                row.get("event_game_id"),
                bool(event_ended) if event_ended is not None else None,
                row.get("condition_id"),
                row.get("sports_market_type"),
                row.get("game_start_time"),
                row.get("group_item_title"),
                row.get("tags_str"),
                row.get("clob_token_ids"),
                bool(is_resolved) if is_resolved is not None else None,
                row.get("winning_outcome"),
                row.get("winning_clob_token_id"),
            )
        )

        toks = row.get("clobTokenIds_str")
        if toks and toks != "[]":
            token_data.append((market_id, toks))

    return market_data, token_data
