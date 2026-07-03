"""
Data shaping utilities for raw market ingestion.

Holds transformation helpers that operate on raw market payloads before they
are persisted to storage. Kept separate from fetch/persistence to keep the
responsibility focused.
"""

import json
from typing import Dict, List

import polars as pl


def _normalize_nested_value(value):
    if isinstance(value, pl.Series):
        return value.to_list()
    if isinstance(value, tuple):
        return list(value)
    return value


def _jsonify_nested_value(value) -> str:
    value = _normalize_nested_value(value)
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)


def _jsonify_optional_nested_value(value) -> str | None:
    value = _normalize_nested_value(value)
    if value is None:
        return None
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)


def extract_event_slug(events) -> str:
    """Extract event slug from the events field."""
    events = _normalize_nested_value(events)
    if events is None:
        return None
    if isinstance(events, list) and len(events) > 0:
        first_event = events[0]
        if isinstance(first_event, dict):
            return first_event.get("slug")
    return None


def extract_event_id(events) -> str:
    """Extract parent event id from the events field."""
    events = _normalize_nested_value(events)
    if events is None:
        return None
    if isinstance(events, list) and len(events) > 0:
        first_event = events[0]
        if isinstance(first_event, dict):
            raw = first_event.get("id")
            return str(raw) if raw is not None else None
    return None


def process_markets_dataframe(markets_list: List[Dict]) -> pl.DataFrame:
    """Process raw markets list into cleaned Polars DataFrame."""
    if not markets_list:
        return pl.DataFrame()

    # Keep only fields used downstream so schema drift in unrelated API keys
    # cannot break frame construction.
    relevant_keys = [
        "id",
        "question",
        "category",
        "description",
        "outcomes",
        "volumeNum",
        "active",
        "closed",
        "createdAt",
        "endDate",
        "conditionId",
        "condition_id",
        "sportsMarketType",
        "sports_market_type",
        "gameStartTime",
        "game_start_time",
        "groupItemTitle",
        "group_item_title",
        "tags",
        "resolved",
        "isResolved",
        "winningOutcome",
        "winning_outcome",
        "winningClobTokenId",
        "winning_clob_token_id",
        "clobTokenIds",
        "slug",
        "events",
    ]
    trimmed_rows = [
        {key: market.get(key) for key in relevant_keys}
        for market in markets_list
        if isinstance(market, dict)
    ]

    # Infer across the full page to tolerate mixed-type rows from Gamma.
    df = pl.from_dicts(trimmed_rows, infer_schema_length=None)

    df = df.with_columns(
        [
            # Convert outcomes list to string for storage
            pl.col("outcomes")
            .map_elements(
                _jsonify_nested_value,
                return_dtype=pl.Utf8,
            )
            .alias("outcomes_str"),
            # Convert clobTokenIds list to string for storage
            pl.col("clobTokenIds")
            .map_elements(
                _jsonify_nested_value,
                return_dtype=pl.Utf8,
            )
            .alias("clobTokenIds_str"),
            pl.col("clobTokenIds")
            .map_elements(
                _jsonify_optional_nested_value,
                return_dtype=pl.Utf8,
            )
            .alias("clob_token_ids"),
            pl.col("tags")
            .map_elements(
                _jsonify_optional_nested_value,
                return_dtype=pl.Utf8,
            )
            .alias("tags_str"),
            pl.coalesce([pl.col("conditionId"), pl.col("condition_id")])
            .cast(pl.Utf8, strict=False)
            .alias("condition_id"),
            pl.coalesce([pl.col("sportsMarketType"), pl.col("sports_market_type")])
            .cast(pl.Utf8, strict=False)
            .alias("sports_market_type"),
            pl.coalesce([pl.col("groupItemTitle"), pl.col("group_item_title")])
            .cast(pl.Utf8, strict=False)
            .alias("group_item_title"),
            pl.coalesce([pl.col("winningOutcome"), pl.col("winning_outcome")])
            .cast(pl.Utf8, strict=False)
            .alias("winning_outcome"),
            pl.coalesce([pl.col("winningClobTokenId"), pl.col("winning_clob_token_id")])
            .cast(pl.Utf8, strict=False)
            .alias("winning_clob_token_id"),
            pl.coalesce([pl.col("resolved"), pl.col("isResolved")])
            .cast(pl.Boolean, strict=False)
            .alias("is_resolved"),
            # Extract event_slug from events field
            pl.col("events")
            .map_elements(
                extract_event_slug,
                return_dtype=pl.Utf8,
            )
            .alias("event_slug"),
            pl.col("events")
            .map_elements(
                extract_event_id,
                return_dtype=pl.Utf8,
            )
            .alias("event_id"),
            # Parse dates
            pl.col("createdAt")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
            .alias("created_at"),
            pl.col("endDate")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
            .alias("end_date"),
            pl.coalesce([pl.col("gameStartTime"), pl.col("game_start_time")])
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
            .alias("game_start_time"),
            # Ensure volumeNum is float
            pl.col("volumeNum").cast(pl.Float64).fill_null(0.0),
            # Ensure boolean columns are properly typed
            pl.col("active").cast(pl.Boolean),
            pl.col("closed").cast(pl.Boolean),
        ]
    )

    return df
