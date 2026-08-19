"""Complete, cumulative Polymarket event/market catalog acquisition."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any, Final

from oddsfox_pipeline.ingestion.polymarket.errors import gamma_get
from oddsfox_pipeline.ingestion.polymarket.markets.fetch import build_client
from oddsfox_pipeline.storage.duckdb.polymarket_catalog import (
    activate_catalog_crawl,
    catalog_crawl_pages,
    catalog_crawl_status,
    delete_catalog_pass,
    record_catalog_issue,
    save_catalog_page,
    start_catalog_crawl,
)

CATALOG_CONTRACT_VERSION: Final = "oddsfox.polymarket.graph-catalog.v1"
TRADABILITY_PREDICATE_VERSION: Final = "1.0.0"
CATALOG_PASSES: Final = (
    ("events_open", "/events/keyset", "events", False),
    ("events_closed", "/events/keyset", "events", True),
    ("markets_open", "/markets/keyset", "markets", False),
    ("markets_closed", "/markets/keyset", "markets", True),
)
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
_SOURCE_ID = re.compile(r"^[0-9]+$")


class CatalogConflictError(ValueError):
    """Raised when one source ID has contradictory durable identity fields."""


def _text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _source_text(value: Any) -> str | None:
    """Retain source prose byte-for-byte after JSON has decoded it."""
    if value is None:
        return None
    return str(value)


def _source_id(value: Any, kind: str) -> str:
    text = _text(value)
    if isinstance(value, bool) or text is None or not _SOURCE_ID.fullmatch(text):
        raise CatalogConflictError(f"malformed {kind} ID")
    return text


def _bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _timestamp(value: Any) -> str | None:
    text = _text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (
        parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)
        .astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def clean_source_text(value: Any) -> str | None:
    """Normalize derived text without altering the separately retained source field."""
    text = _text(value)
    if text is None:
        return None
    text = unicodedata.normalize("NFKC", text).replace("\r\n", "\n").replace("\r", "\n")
    return _CONTROL_CHARACTERS.sub("", text)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _clean_derived_value(value: Any) -> Any:
    if isinstance(value, str):
        return clean_source_text(value) or ""
    if isinstance(value, list):
        return [_clean_derived_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _clean_derived_value(item) for key, item in value.items()}
    return value


def _derived_json(value: Any) -> str:
    return canonical_json(_clean_derived_value(value))


def _json_list(value: Any, field: str) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise CatalogConflictError(f"malformed {field}") from exc
        if isinstance(parsed, list):
            return parsed
    raise CatalogConflictError(f"malformed {field}")


def _clob_token_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise CatalogConflictError("malformed clob_token_ids") from exc
    if not isinstance(value, list):
        raise CatalogConflictError("malformed clob_token_ids")
    tokens = [_text(item) for item in value if not isinstance(item, bool)]
    if len(tokens) != len(value) or any(item is None for item in tokens):
        raise CatalogConflictError("malformed clob_token_ids")
    return [item for item in tokens if item is not None]


def _objects(value: Any, keys: tuple[str, ...], *, field: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _json_list(value, field):
        if isinstance(item, Mapping):
            row = {
                key: _clean_derived_value(item.get(key))
                for key in keys
                if item.get(key) is not None
            }
            if row:
                rows.append(row)
        elif _text(item):
            rows.append({"label": clean_source_text(item)})
    return [json.loads(item) for item in sorted({canonical_json(row) for row in rows})]


def tradability_evidence(market: Mapping[str, Any]) -> tuple[str, ...]:
    evidence: list[str] = []
    token_ids = (
        market.get("clobTokenIds")
        if "clobTokenIds" in market
        else market.get("clob_token_ids")
    )
    if _clob_token_ids(token_ids):
        evidence.append("clob_token_ids")
    if market.get("enableOrderBook") is True:
        evidence.append("enable_order_book")
    if _timestamp(market.get("acceptingOrdersTimestamp")):
        evidence.append("accepting_orders_timestamp")
    if _timestamp(market.get("fundedTimestamp")):
        evidence.append("funded_timestamp")
    condition_id = _text(market.get("conditionId") or market.get("condition_id"))
    if condition_id and (market.get("ready") is True or market.get("funded") is True):
        evidence.append("condition_deployed")
    return tuple(evidence)


def _content_text(kind: str, row: Mapping[str, Any]) -> str:
    labels: list[tuple[str, Any]] = [("Type", kind), ("ID", row[f"{kind}_id"])]
    if kind == "event":
        labels.extend(
            (
                ("Title", row.get("title")),
                ("Subtitle", row.get("subtitle")),
                ("Description", row.get("description")),
                ("Resolution source", row.get("resolution_source")),
                ("Category", row.get("category")),
                (
                    "Tags",
                    ", ".join(
                        item.get("label") or item.get("slug") or item.get("id", "")
                        for item in json.loads(row["tags_json"])
                    ),
                ),
                (
                    "Series",
                    ", ".join(
                        item.get("title") or item.get("slug") or item.get("id", "")
                        for item in json.loads(row["series_json"])
                    ),
                ),
            )
        )
    else:
        labels.extend(
            (
                ("Question", row.get("title")),
                ("Description", row.get("description")),
                ("Resolution source", row.get("resolution_source")),
                ("Category", row.get("category")),
                (
                    "Outcomes",
                    ", ".join(str(item) for item in json.loads(row["outcomes_json"])),
                ),
                (
                    "Tags",
                    ", ".join(
                        item.get("label") or item.get("slug") or item.get("id", "")
                        for item in json.loads(row["tags_json"])
                    ),
                ),
            )
        )
    labels.extend(
        (
            (
                "Status",
                ", ".join(
                    name
                    for name, value in (
                        ("active", row.get("is_active")),
                        ("closed", row.get("is_closed")),
                        ("archived", row.get("is_archived")),
                        ("resolved", row.get("is_resolved")),
                        ("tradable", row.get("is_tradable")),
                    )
                    if value is True
                ),
            ),
            ("Start", row.get("start_at")),
            ("End", row.get("end_at")),
            ("Closed", row.get("closed_at")),
        )
    )
    return "\n".join(
        f"{label}: {clean_source_text(value)}"
        for label, value in labels
        if clean_source_text(value)
    )


def _event_row(
    event: Mapping[str, Any], *, crawl_id: str, observed_at: str
) -> dict[str, Any]:
    event_id = _source_id(event.get("id"), "event")
    row = {
        "crawl_id": crawl_id,
        "observed_at": observed_at,
        "event_id": event_id,
        "title": _source_text(event.get("title")),
        "subtitle": _source_text(event.get("subtitle")),
        "description": _source_text(event.get("description")),
        "resolution_source": _source_text(event.get("resolutionSource")),
        "slug": _text(event.get("slug")),
        "category": _text(event.get("category")),
        "tags_json": _derived_json(
            _objects(event.get("tags"), ("id", "slug", "label"), field="event tags")
        ),
        "series_json": _derived_json(
            _objects(
                event.get("series"),
                ("id", "slug", "title", "ticker"),
                field="event series",
            )
        ),
        "is_active": _bool(event.get("active")),
        "is_closed": _bool(event.get("closed")),
        "is_archived": _bool(event.get("archived")),
        "is_resolved": _bool(event.get("resolved")),
        "source_created_at": _timestamp(
            event.get("createdAt") or event.get("creationDate")
        ),
        "source_updated_at": _timestamp(event.get("updatedAt")),
        "start_at": _timestamp(event.get("startDate") or event.get("startTime")),
        "end_at": _timestamp(event.get("endDate")),
        "closed_at": _timestamp(
            event.get("closedTime") or event.get("finishedTimestamp")
        ),
        "attributes_json": _derived_json(
            {
                key: event.get(key)
                for key in (
                    "ticker",
                    "subcategory",
                    "gameId",
                    "parentEventId",
                    "negRisk",
                    "enableNegRisk",
                    "restricted",
                )
                if event.get(key) is not None
            }
        ),
        "source_priority": 2,
    }
    row["content_text"] = _content_text("event", row)
    row["content_text_sha256"] = hashlib.sha256(
        row["content_text"].encode()
    ).hexdigest()
    return row


def _market_row(
    market: Mapping[str, Any], *, crawl_id: str, observed_at: str
) -> dict[str, Any]:
    market_id = _source_id(market.get("id"), "market")
    evidence = tradability_evidence(market)
    row = {
        "crawl_id": crawl_id,
        "observed_at": observed_at,
        "market_id": market_id,
        "title": _source_text(market.get("question")),
        "subtitle": _source_text(market.get("groupItemTitle")),
        "description": _source_text(market.get("description")),
        "resolution_source": _source_text(market.get("resolutionSource")),
        "slug": _text(market.get("slug")),
        "category": _text(market.get("category")),
        "tags_json": _derived_json(
            _objects(
                market.get("tags"),
                ("id", "slug", "label"),
                field="market tags",
            )
        ),
        "outcomes_json": _derived_json(_json_list(market.get("outcomes"), "outcomes")),
        "tradability_evidence_json": _derived_json(list(evidence)),
        "is_active": _bool(market.get("active")),
        "is_closed": _bool(market.get("closed")),
        "is_archived": _bool(market.get("archived")),
        "is_resolved": _bool(
            market.get("resolved") if "resolved" in market else market.get("isResolved")
        ),
        "is_tradable": bool(evidence),
        "source_created_at": _timestamp(market.get("createdAt")),
        "source_updated_at": _timestamp(market.get("updatedAt")),
        "start_at": _timestamp(market.get("startDate") or market.get("eventStartTime")),
        "end_at": _timestamp(market.get("endDate")),
        "closed_at": _timestamp(market.get("closedTime")),
        "condition_id": _text(market.get("conditionId") or market.get("condition_id")),
        "attributes_json": _derived_json(
            {
                key: market.get(key)
                for key in (
                    "questionID",
                    "sportsMarketType",
                    "gameId",
                    "line",
                    "groupItemThreshold",
                    "clobTokenIds",
                    "enableOrderBook",
                    "acceptingOrdersTimestamp",
                    "fundedTimestamp",
                    "ready",
                    "funded",
                    "negRisk",
                    "negRiskMarketID",
                    "restricted",
                )
                if market.get(key) is not None
            }
        ),
        "source_priority": 2,
    }
    row["content_text"] = _content_text("market", row)
    row["content_text_sha256"] = hashlib.sha256(
        row["content_text"].encode()
    ).hexdigest()
    return row


def _merge_rows(
    current: dict[str, Any] | None,
    incoming: dict[str, Any],
    *,
    durable_key: str | None = None,
) -> dict[str, Any]:
    if current is None:
        return incoming
    if (
        durable_key
        and current.get(durable_key)
        and incoming.get(durable_key)
        and current[durable_key] != incoming[durable_key]
    ):
        raise CatalogConflictError(
            f"conflicting {durable_key} for source ID {incoming.get('market_id') or incoming.get('event_id')}"
        )

    def rank(row: Mapping[str, Any]) -> tuple[int, str, str]:
        return (
            int(row["source_priority"]),
            str(row.get("source_updated_at") or ""),
            canonical_json(dict(row)),
        )

    preferred, fallback = (
        (incoming, current) if rank(incoming) > rank(current) else (current, incoming)
    )
    merged = {
        key: preferred.get(key) if preferred.get(key) is not None else fallback.get(key)
        for key in set(preferred) | set(fallback)
    }
    if "tradability_evidence_json" in merged:
        evidence = sorted(
            {
                str(item)
                for row in (current, incoming)
                for item in json.loads(row["tradability_evidence_json"])
            }
        )
        merged["tradability_evidence_json"] = _derived_json(evidence)
        merged["is_tradable"] = bool(evidence)
    return merged


def normalize_catalog_pages(
    pages: Iterable[Mapping[str, Any]], *, crawl_id: str, observed_at: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    events: dict[str, dict[str, Any]] = {}
    markets: dict[str, dict[str, Any]] = {}
    edges: dict[tuple[str, str], set[str]] = {}
    for page in pages:
        pass_name = str(page["pass_name"])
        if pass_name not in {item[0] for item in CATALOG_PASSES}:
            raise ValueError(f"unrecognized catalog pass: {pass_name}")
        payload = json.loads(str(page["payload_json"]))
        if not isinstance(payload, Mapping):
            raise ValueError(f"catalog pass {pass_name} returned a non-object payload")
        result_key = "events" if pass_name.startswith("events_") else "markets"
        source_rows = payload.get(result_key, [])
        if not isinstance(source_rows, list):
            raise ValueError(
                f"catalog pass {pass_name} returned non-array {result_key}"
            )
        for source in source_rows:
            if not isinstance(source, Mapping):
                raise ValueError(f"catalog pass {pass_name} returned a non-object row")
            if pass_name.startswith("events_"):
                event = _event_row(source, crawl_id=crawl_id, observed_at=observed_at)
                events[event["event_id"]] = _merge_rows(
                    events.get(event["event_id"]), event
                )
                nested_markets = source.get("markets") or []
                if not isinstance(nested_markets, list):
                    raise ValueError("event row returned non-array markets")
                for nested in nested_markets:
                    if not isinstance(nested, Mapping):
                        raise ValueError("event row returned a non-object market")
                    market = _market_row(
                        nested, crawl_id=crawl_id, observed_at=observed_at
                    )
                    market["source_priority"] = 1
                    markets[market["market_id"]] = _merge_rows(
                        markets.get(market["market_id"]),
                        market,
                        durable_key="condition_id",
                    )
                    edges.setdefault(
                        (event["event_id"], market["market_id"]), set()
                    ).add("events_endpoint")
            else:
                market = _market_row(source, crawl_id=crawl_id, observed_at=observed_at)
                markets[market["market_id"]] = _merge_rows(
                    markets.get(market["market_id"]),
                    market,
                    durable_key="condition_id",
                )
                nested_events = source.get("events") or []
                if not isinstance(nested_events, list):
                    raise ValueError("market row returned non-array events")
                for nested in nested_events:
                    if not isinstance(nested, Mapping):
                        raise ValueError("market row returned a non-object event")
                    event = _event_row(
                        nested, crawl_id=crawl_id, observed_at=observed_at
                    )
                    event["source_priority"] = 1
                    events[event["event_id"]] = _merge_rows(
                        events.get(event["event_id"]), event
                    )
                    edges.setdefault(
                        (event["event_id"], market["market_id"]), set()
                    ).add("markets_endpoint")
    for kind, rows in (("event", events.values()), ("market", markets.values())):
        for row in rows:
            row.pop("source_priority", None)
            row["content_text"] = _content_text(kind, row)
            row["content_text_sha256"] = hashlib.sha256(
                row["content_text"].encode()
            ).hexdigest()
    edge_rows = []
    for (event_id, market_id), evidence in sorted(edges.items()):
        content_text = (
            f'Polymarket event "{clean_source_text(events[event_id].get("title")) or event_id}" '
            f'contains market "{clean_source_text(markets[market_id].get("title")) or market_id}".'
        )
        edge_rows.append(
            {
                "crawl_id": crawl_id,
                "observed_at": observed_at,
                "event_id": event_id,
                "market_id": market_id,
                "evidence_json": _derived_json(sorted(evidence)),
                "content_text": content_text,
                "content_text_sha256": hashlib.sha256(
                    content_text.encode()
                ).hexdigest(),
            }
        )
    return (
        [events[key] for key in sorted(events)],
        [markets[key] for key in sorted(markets)],
        edge_rows,
    )


def _fetch_pass(
    conn,
    client: Any,
    *,
    crawl_id: str,
    pass_name: str,
    endpoint: str,
    result_key: str,
    closed: bool,
    max_pages: int | None,
) -> None:
    existing = catalog_crawl_pages(conn, crawl_id, pass_name)
    cursor = existing[-1]["next_cursor"] if existing else None
    page_number = len(existing)
    if existing and existing[-1]["is_complete"]:
        return
    while True:
        params: dict[str, Any] = {
            "limit": 500 if result_key == "events" else 100,
            "closed": closed,
        }
        if result_key == "markets":
            params["include_tag"] = True
        if cursor:
            params["after_cursor"] = cursor
        payload = gamma_get(client, endpoint, params=params)
        if not isinstance(payload, Mapping):
            raise ValueError(f"catalog pass {pass_name} returned a non-object payload")
        rows = payload.get(result_key, [])
        if not isinstance(rows, list):
            raise ValueError(
                f"catalog pass {pass_name} returned non-array {result_key}"
            )
        next_cursor = payload.get("next_cursor")
        if next_cursor is not None and (
            not isinstance(next_cursor, str) or not next_cursor
        ):
            raise ValueError(f"catalog pass {pass_name} returned a malformed cursor")
        if cursor and next_cursor == cursor:
            raise RuntimeError(f"non-advancing Gamma cursor for {pass_name}")
        if next_cursor and not rows:
            raise RuntimeError(f"Gamma returned an unresolved cursor for {pass_name}")
        is_complete = next_cursor is None
        save_catalog_page(
            conn,
            crawl_id=crawl_id,
            pass_name=pass_name,
            page_number=page_number,
            payload=payload,
            next_cursor=next_cursor,
            is_complete=is_complete,
        )
        page_number += 1
        if is_complete:
            return
        if max_pages is not None and page_number >= max_pages:
            raise RuntimeError(
                f"catalog pass {pass_name} reached max_pages before completion"
            )
        cursor = next_cursor


def collect_polymarket_catalog(
    conn,
    *,
    crawl_id: str | None = None,
    max_pages: int | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    """Collect and atomically activate all four global Gamma catalog passes."""
    crawl_id = crawl_id or uuid.uuid4().hex
    status = catalog_crawl_status(conn, crawl_id)
    if status and status["status"] == "complete":
        return status["summary"]
    observed_at = start_catalog_crawl(conn, crawl_id)
    owns_client = client is None
    http = client or build_client()
    try:
        for pass_name, endpoint, result_key, closed in CATALOG_PASSES:
            try:
                _fetch_pass(
                    conn,
                    http,
                    crawl_id=crawl_id,
                    pass_name=pass_name,
                    endpoint=endpoint,
                    result_key=result_key,
                    closed=closed,
                    max_pages=max_pages,
                )
            except Exception:
                if catalog_crawl_pages(conn, crawl_id, pass_name):
                    delete_catalog_pass(conn, crawl_id, pass_name)
                    _fetch_pass(
                        conn,
                        http,
                        crawl_id=crawl_id,
                        pass_name=pass_name,
                        endpoint=endpoint,
                        result_key=result_key,
                        closed=closed,
                        max_pages=max_pages,
                    )
                else:
                    raise
        pages = [
            row
            for pass_name, *_ in CATALOG_PASSES
            for row in catalog_crawl_pages(conn, crawl_id, pass_name)
        ]
        events, markets, edges = normalize_catalog_pages(
            pages, crawl_id=crawl_id, observed_at=observed_at
        )
        summary = activate_catalog_crawl(
            conn,
            crawl_id=crawl_id,
            event_rows=events,
            market_rows=markets,
            edge_rows=edges,
        )
        return summary
    except Exception as exc:
        if isinstance(exc, ValueError):
            record_catalog_issue(
                conn,
                crawl_id=crawl_id,
                issue_type=exc.__class__.__name__,
                detail=str(exc),
            )
        start_catalog_crawl(conn, crawl_id, error_type=exc.__class__.__name__)
        raise
    finally:
        if owns_client:
            http.session.close()


__all__ = [
    "CATALOG_CONTRACT_VERSION",
    "CatalogConflictError",
    "TRADABILITY_PREDICATE_VERSION",
    "canonical_json",
    "clean_source_text",
    "collect_polymarket_catalog",
    "normalize_catalog_pages",
    "tradability_evidence",
]
