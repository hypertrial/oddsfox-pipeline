"""
Data shaping utilities for raw market ingestion.

Holds transformation helpers that operate on raw market payloads before they
are persisted to storage. Kept separate from fetch/persistence to keep the
responsibility focused.
"""

import json
from datetime import date, datetime, timezone
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


def _preferred_event(events) -> dict | None:
    """Prefer the enclosing event when marked; otherwise the first dict entry."""
    events = _normalize_nested_value(events)
    if not isinstance(events, list):
        return None
    enclosing = next(
        (
            event
            for event in events
            if isinstance(event, dict) and event.get("is_enclosing_event")
        ),
        None,
    )
    if enclosing is not None:
        return enclosing
    return next((event for event in events if isinstance(event, dict)), None)


def extract_event_slug(events) -> str:
    """Extract enclosing (or first) event slug from the events field."""
    preferred = _preferred_event(events)
    if preferred is None:
        return None
    return preferred.get("slug")


def _as_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_gamma_datetime_value(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _as_naive_utc(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text, "%Y-%m-%d")
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        clean = normalized.replace("T", " ")
        if "." in clean:
            clean = clean.split(".", 1)[0]
        if "+" in clean:
            clean = clean.split("+", 1)[0].rstrip()
        try:
            parsed = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_naive_utc(parsed)


def _parse_gamma_datetime_expr(column: str, *, alias: str) -> pl.Expr:
    return (
        pl.col(column)
        .map_elements(_parse_gamma_datetime_value, return_dtype=pl.Datetime)
        .alias(alias)
    )


def _parse_gamma_datetime_from_expr(expr: pl.Expr, *, alias: str) -> pl.Expr:
    return expr.map_elements(
        _parse_gamma_datetime_value, return_dtype=pl.Datetime
    ).alias(alias)


def extract_event_id(events) -> str:
    """Extract enclosing (or first) parent event id from the events field."""
    preferred = _preferred_event(events)
    if preferred is None:
        return None
    raw = preferred.get("id")
    return str(raw) if raw is not None else None


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
        "resolutionSource",
        "outcomes",
        "volume",
        "volumeNum",
        "active",
        "closed",
        "createdAt",
        "endDate",
        "endDateIso",
        "conditionId",
        "condition_id",
        "sportsMarketType",
        "sports_market_type",
        "gameStartTime",
        "game_start_time",
        "groupItemTitle",
        "group_item_title",
        "groupItemThreshold",
        "group_item_threshold",
        "line",
        "tags",
        "resolved",
        "isResolved",
        "winningOutcome",
        "winning_outcome",
        "winningClobTokenId",
        "winning_clob_token_id",
        "negRiskMarketID",
        "neg_risk_market_id",
        "negRiskRequestID",
        "neg_risk_request_id",
        "negRiskOther",
        "neg_risk_other",
        "clobTokenIds",
        "slug",
        "events",
        "eventTitle",
        "event_title",
        "eventStartTime",
        "event_start_time",
        "eventFinishedTime",
        "event_finished_time",
        "eventGameId",
        "event_game_id",
        "eventEnded",
        "event_ended",
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
            pl.col("resolutionSource")
            .cast(pl.Utf8, strict=False)
            .alias("market_resolution_source"),
            pl.coalesce([pl.col("sportsMarketType"), pl.col("sports_market_type")])
            .cast(pl.Utf8, strict=False)
            .alias("sports_market_type"),
            pl.coalesce([pl.col("groupItemTitle"), pl.col("group_item_title")])
            .cast(pl.Utf8, strict=False)
            .alias("group_item_title"),
            pl.coalesce([pl.col("groupItemThreshold"), pl.col("group_item_threshold")])
            .cast(pl.Utf8, strict=False)
            .alias("group_item_threshold"),
            pl.col("line").cast(pl.Float64, strict=False).alias("line"),
            pl.coalesce([pl.col("winningOutcome"), pl.col("winning_outcome")])
            .cast(pl.Utf8, strict=False)
            .alias("winning_outcome"),
            pl.coalesce([pl.col("winningClobTokenId"), pl.col("winning_clob_token_id")])
            .cast(pl.Utf8, strict=False)
            .alias("winning_clob_token_id"),
            pl.coalesce([pl.col("negRiskMarketID"), pl.col("neg_risk_market_id")])
            .cast(pl.Utf8, strict=False)
            .alias("neg_risk_market_id"),
            pl.coalesce([pl.col("negRiskRequestID"), pl.col("neg_risk_request_id")])
            .cast(pl.Utf8, strict=False)
            .alias("neg_risk_request_id"),
            pl.coalesce([pl.col("negRiskOther"), pl.col("neg_risk_other")])
            .cast(pl.Boolean, strict=False)
            .alias("neg_risk_other"),
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
            pl.coalesce([pl.col("eventTitle"), pl.col("event_title")])
            .cast(pl.Utf8, strict=False)
            .alias("event_title"),
            pl.coalesce([pl.col("eventGameId"), pl.col("event_game_id")])
            .cast(pl.Utf8, strict=False)
            .alias("event_game_id"),
            pl.coalesce([pl.col("eventEnded"), pl.col("event_ended")])
            .cast(pl.Boolean, strict=False)
            .alias("event_ended"),
            _parse_gamma_datetime_expr("createdAt", alias="created_at"),
            _parse_gamma_datetime_from_expr(
                pl.coalesce([pl.col("endDate"), pl.col("endDateIso")]),
                alias="end_date",
            ),
            _parse_gamma_datetime_from_expr(
                pl.coalesce([pl.col("gameStartTime"), pl.col("game_start_time")]),
                alias="game_start_time",
            ),
            _parse_gamma_datetime_from_expr(
                pl.coalesce([pl.col("eventStartTime"), pl.col("event_start_time")]),
                alias="event_start_time",
            ),
            _parse_gamma_datetime_from_expr(
                pl.coalesce(
                    [pl.col("eventFinishedTime"), pl.col("event_finished_time")]
                ),
                alias="event_finished_time",
            ),
            pl.coalesce(
                [
                    pl.col("volumeNum").cast(pl.Float64, strict=False),
                    pl.col("volume").cast(pl.Float64, strict=False),
                ]
            ).alias("volumeNum"),
            # Ensure boolean columns are properly typed
            pl.col("active").cast(pl.Boolean),
            pl.col("closed").cast(pl.Boolean),
        ]
    )

    return df
