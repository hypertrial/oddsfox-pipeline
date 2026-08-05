"""Land event-catalog market payloads into dlt-owned raw markets tables."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from oddsfox_pipeline.ingestion.polymarket.markets.persistence import (
    prepare_batch_for_db,
)
from oddsfox_pipeline.ingestion.polymarket.markets.transform import (
    process_markets_dataframe,
)
from oddsfox_pipeline.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox_pipeline.storage.duckdb.markets import save_market_tokens_batch
from oddsfox_pipeline.storage.duckdb.polymarket_scope import active_polymarket_scope
from oddsfox_pipeline.storage.duckdb.registry_common import _normalize_scope
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

logger = logging.getLogger(__name__)

_PAYLOAD_COLUMNS = (
    "market_id",
    "question",
    "category",
    "description",
    "market_resolution_source",
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
    "group_item_threshold",
    "line",
    "tags",
    "clob_token_ids",
    "is_resolved",
    "winning_outcome",
    "winning_clob_token_id",
    "neg_risk_market_id",
    "neg_risk_request_id",
    "neg_risk_other",
)


def _latest_registry_payload_sql(scope_name: str) -> str:
    payloads = polymarket_raw_tbl(scope_name, "event_market_payload_snapshots")
    registry = polymarket_ops_tbl(scope_name, "market_scope_registry")
    columns = ", ".join(f"p.{column}" for column in _PAYLOAD_COLUMNS)
    return f"""
        SELECT {columns}, p.observed_at
        FROM {payloads} AS p
        INNER JOIN {registry} AS r
            ON p.market_id = r.market_id
        WHERE
            lower(r.scope_name) = ?
            AND coalesce(r.is_event_volume_eligible, false)
        QUALIFY row_number() OVER (
            PARTITION BY p.market_id
            ORDER BY p.observed_at DESC
        ) = 1
    """


def _format_gamma_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
    return str(value)


def _parse_json_list(value: Any) -> list[Any] | str | None:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return stripped
            return parsed if isinstance(parsed, list) else stripped
        return stripped
    return value


def _payload_row_to_gamma_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    payload = dict(zip(_PAYLOAD_COLUMNS, row, strict=True))
    market_id = str(payload.pop("market_id"))
    payload.pop("scraped_at", None)
    event_slug = payload.get("event_slug")
    event_id = payload.get("event_id")
    events = None
    if event_slug or event_id is not None:
        events = [
            {
                "slug": event_slug,
                "id": str(event_id) if event_id is not None else None,
            }
        ]
    return {
        "id": market_id,
        "question": payload.get("question"),
        "category": payload.get("category"),
        "description": payload.get("description"),
        "resolutionSource": payload.get("market_resolution_source"),
        "outcomes": _parse_json_list(payload.get("outcomes")),
        "volumeNum": payload.get("volume"),
        "active": payload.get("active"),
        "closed": payload.get("closed"),
        "createdAt": _format_gamma_timestamp(payload.get("created_at")),
        "endDate": _format_gamma_timestamp(payload.get("end_date")),
        "slug": payload.get("slug"),
        "condition_id": payload.get("condition_id"),
        "sports_market_type": payload.get("sports_market_type"),
        "game_start_time": _format_gamma_timestamp(payload.get("game_start_time")),
        "group_item_title": payload.get("group_item_title"),
        "group_item_threshold": payload.get("group_item_threshold"),
        "line": payload.get("line"),
        "tags": _parse_json_list(payload.get("tags")),
        "clobTokenIds": _parse_json_list(payload.get("clob_token_ids")),
        "isResolved": payload.get("is_resolved"),
        "winning_outcome": payload.get("winning_outcome"),
        "winning_clob_token_id": payload.get("winning_clob_token_id"),
        "neg_risk_market_id": payload.get("neg_risk_market_id"),
        "neg_risk_request_id": payload.get("neg_risk_request_id"),
        "neg_risk_other": payload.get("neg_risk_other"),
        "event_title": payload.get("event_title"),
        "event_start_time": _format_gamma_timestamp(payload.get("event_start_time")),
        "event_finished_time": _format_gamma_timestamp(
            payload.get("event_finished_time")
        ),
        "event_game_id": payload.get("event_game_id"),
        "event_ended": payload.get("event_ended"),
        "events": events,
    }


def materialize_registry_markets_from_event_catalog(
    *,
    scope_name: str = DEFAULT_MARKET_SCOPE,
) -> dict[str, int]:
    """Merge latest event-catalog payloads for admitted registry markets into raw tables."""
    scope = _normalize_scope(scope_name)
    ensure_duck_db()
    with active_polymarket_scope(scope):
        with get_connection() as conn:
            rows = conn.execute(
                _latest_registry_payload_sql(scope),
                [scope],
            ).fetchall()
        if not rows:
            return {"markets_materialized": 0, "token_rows_materialized": 0}

        from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
            normalize_market_payloads_for_dlt,
            polymarket_wc2026_markets_source,
        )
        from oddsfox_pipeline.storage.duckdb.dlt_batch import (
            get_polymarket_dlt_pipeline,
        )

        market_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = _payload_row_to_gamma_dict(row[:-1])
            market_rows.extend(
                normalize_market_payloads_for_dlt(
                    [payload],
                    observed_at=row[-1],
                )
            )
        df = process_markets_dataframe(
            [_payload_row_to_gamma_dict(row[:-1]) for row in rows]
        )
        _, token_data = prepare_batch_for_db(df)
        token_rows = list(token_data)

        pipeline = get_polymarket_dlt_pipeline(scope_name=scope)
        if pipeline.has_pending_data:
            pipeline.drop_pending_packages()
        pipeline.run(polymarket_wc2026_markets_source(rows=market_rows))
        save_market_tokens_batch(token_rows, scope_name=scope)
        logger.info(
            "Materialized %s registry markets and %s token rows from event catalog",
            len(market_rows),
            len(token_rows),
        )
        return {
            "markets_materialized": len(market_rows),
            "token_rows_materialized": len(token_rows),
        }


__all__ = ["materialize_registry_markets_from_event_catalog"]
