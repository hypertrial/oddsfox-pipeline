"""Event-grain WC2026 catalog ingestion from Polymarket Gamma.

The logical contract admits complete Polymarket events by Gamma's reported
cumulative event volume. Event admission therefore remains distinct from the
platform-wide market catalog's market-grain contract.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Callable, Iterable

from oddsfox_pipeline.config.settings_polymarket import (
    POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
)
from oddsfox_pipeline.ingestion.polymarket.errors import gamma_get
from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
    fetch_gamma_event_by_id,
    iter_gamma_events_keyset,
)
from oddsfox_pipeline.ingestion.polymarket.markets.fetch import build_client

WC2026_EVENT_TAG = "2026-fifa-world-cup"
WC2026_RECALL_TAG = "fifa-world-cup"
WC2026_FIXTURE_SERIES_SLUG = "soccer-fifwc"
WC2026_RECALL_EVENT_SLUG_PREFIXES = ("2026-fifa-world-cup", "fifwc-")
SCAN_CONVERGENCE_ATTEMPTS = 3


@dataclass(frozen=True)
class EventCatalogBatch:
    event_snapshots: tuple[dict[str, Any], ...]
    event_tag_snapshots: tuple[dict[str, Any], ...]
    event_market_snapshots: tuple[dict[str, Any], ...]
    market_payloads: tuple[dict[str, Any], ...]
    summary: dict[str, Any]


def _text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    return normalized if math.isfinite(normalized) and normalized >= 0 else None


def _bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _tag_rows(event: dict[str, Any], observed_at: datetime) -> list[dict[str, Any]]:
    event_id = _text(event.get("id"))
    if event_id is None:
        return []
    rows: list[dict[str, Any]] = []
    for tag in event.get("tags") or []:
        if not isinstance(tag, dict):
            continue
        tag_slug = _text(tag.get("slug"))
        tag_id = _text(tag.get("id"))
        if tag_id is None and tag_slug is None:
            continue
        rows.append(
            {
                "event_id": event_id,
                "tag_key": tag_id or tag_slug,
                "tag_id": tag_id,
                "tag_slug": tag_slug.lower() if tag_slug else None,
                "tag_label": _text(tag.get("label")),
                "observed_at": observed_at,
            }
        )
    return rows


def _event_tag_slugs(event: dict[str, Any]) -> frozenset[str]:
    return frozenset(
        str(tag.get("slug")).strip().lower()
        for tag in event.get("tags") or []
        if isinstance(tag, dict) and _text(tag.get("slug"))
    )


def _series_slugs(event: dict[str, Any]) -> list[str]:
    slugs = {
        str(series.get("slug")).strip().lower()
        for series in event.get("series") or []
        if isinstance(series, dict) and _text(series.get("slug"))
    }
    direct = _text(event.get("seriesSlug"))
    if direct:
        slugs.add(direct.lower())
    return sorted(slugs)


def _event_snapshot(
    event: dict[str, Any],
    observed_at: datetime,
    candidate_sources: Iterable[str],
    *,
    source_endpoint: str,
) -> dict[str, Any]:
    tags = sorted(_event_tag_slugs(event))
    return {
        "event_id": _text(event.get("id")),
        "event_slug": _text(event.get("slug")),
        "event_title": _text(event.get("title")),
        "event_subtitle": _text(event.get("subtitle")),
        "event_description": _text(event.get("description")),
        "resolution_source": _text(event.get("resolutionSource")),
        "event_volume_usd_lifetime_reported": _float(event.get("volume")),
        "volume_24h_usd": _float(event.get("volume24hr")),
        "volume_1w_usd": _float(event.get("volume1wk")),
        "volume_1m_usd": _float(event.get("volume1mo")),
        "volume_1y_usd": _float(event.get("volume1yr")),
        "liquidity_usd": _float(event.get("liquidity")),
        "open_interest_usd": _float(event.get("openInterest")),
        "is_active": _bool(event.get("active")),
        "is_closed": _bool(event.get("closed")),
        "is_archived": _bool(event.get("archived")),
        "created_at": event.get("createdAt") or event.get("creationDate"),
        "source_updated_at": event.get("updatedAt"),
        "start_at": event.get("startDate"),
        "end_at": event.get("endDate"),
        "closed_at": event.get("closedTime"),
        "event_start_at": event.get("startTime"),
        "finished_at": event.get("finishedTimestamp"),
        "game_id": _text(event.get("gameId")),
        "parent_event_id": _text(
            event.get("parentEventId") or event.get("parentEvent")
        ),
        "neg_risk": _bool(event.get("negRisk")),
        "enable_neg_risk": _bool(event.get("enableNegRisk")),
        "neg_risk_market_id": _text(event.get("negRiskMarketID")),
        "show_all_outcomes": _bool(event.get("showAllOutcomes")),
        "tags_json": json.dumps(tags, separators=(",", ":")),
        "series_slugs_json": json.dumps(_series_slugs(event), separators=(",", ":")),
        "candidate_sources_json": json.dumps(
            sorted(set(candidate_sources)), separators=(",", ":")
        ),
        "source_market_count": sum(
            isinstance(market, dict) and _text(market.get("id")) is not None
            for market in event.get("markets") or []
        ),
        "observed_at": observed_at,
        "source_endpoint": source_endpoint,
    }


def _event_market_rows(
    event: dict[str, Any], observed_at: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    event_id = _text(event.get("id"))
    event_slug = _text(event.get("slug"))
    if event_id is None:
        return [], []
    event_tags = event.get("tags")
    bridge: list[dict[str, Any]] = []
    markets: list[dict[str, Any]] = []
    for market in event.get("markets") or []:
        if not isinstance(market, dict):
            continue
        market_id = _text(market.get("id"))
        if market_id is None:
            continue
        memberships: list[dict[str, Any]] = []
        seen_memberships: set[str] = set()
        for source_ordinal, related in enumerate(market.get("events") or []):
            if not isinstance(related, dict):
                continue
            related_id = _text(related.get("id"))
            if related_id is None or related_id in seen_memberships:
                continue
            seen_memberships.add(related_id)
            memberships.append(
                {
                    "event_id": related_id,
                    "event_slug": _text(related.get("slug")),
                    "source_ordinal": source_ordinal,
                }
            )
        if event_id not in seen_memberships:
            memberships.append(
                {
                    "event_id": event_id,
                    "event_slug": event_slug,
                    "source_ordinal": len(memberships),
                }
            )
        for membership in memberships:
            bridge.append(
                {
                    "event_id": membership["event_id"],
                    "market_id": market_id,
                    "source_ordinal": membership["source_ordinal"],
                    "is_enclosing_event": membership["event_id"] == event_id,
                    "observed_at": observed_at,
                }
            )
        market_payload = {
            **market,
            "events": [
                {
                    "id": item["event_id"],
                    "slug": item["event_slug"],
                    "is_enclosing_event": item["event_id"] == event_id,
                }
                for item in memberships
            ],
            "eventTitle": event.get("title"),
            "eventStartTime": event.get("startTime") or event.get("startDate"),
            "eventFinishedTime": event.get("finishedTimestamp"),
            "eventGameId": event.get("gameId"),
            "eventEnded": event.get("ended"),
        }
        # Gamma nest market payloads omit tags; inherit enclosing event tags.
        if not market.get("tags") and event_tags:
            market_payload["tags"] = event_tags
        markets.append(market_payload)
    return bridge, markets


def _merge_market_payload(
    previous: dict[str, Any] | None, candidate: dict[str, Any]
) -> dict[str, Any]:
    """Keep one deterministic payload while preserving every event reference."""
    if previous is None:
        return candidate
    candidate_score = (
        int(_text(candidate.get("eventGameId")) is not None),
        int(_text(candidate.get("eventTitle")) is not None),
    )
    previous_score = (
        int(_text(previous.get("eventGameId")) is not None),
        int(_text(previous.get("eventTitle")) is not None),
    )
    primary, secondary = (
        (candidate, previous)
        if candidate_score > previous_score
        else (previous, candidate)
    )
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in [*(primary.get("events") or []), *(secondary.get("events") or [])]:
        if not isinstance(item, dict):
            continue
        event_id = _text(item.get("id"))
        if event_id is None or event_id in seen:
            continue
        seen.add(event_id)
        events.append(item)
    # The first payload is selected by sorted enclosing event ID below. Later
    # contexts may only fill missing values; the bridge remains authoritative.
    merged = dict(primary)
    for key, value in secondary.items():
        if merged.get(key) is None and value is not None:
            merged[key] = value
    merged["events"] = events
    return merged


def _fixture_series_id(client: Any) -> str:
    payload = gamma_get(
        client,
        "/series",
        params={"slug": WC2026_FIXTURE_SERIES_SLUG, "limit": 10},
    )
    matches = [
        item
        for item in payload or []
        if isinstance(item, dict)
        and _text(item.get("slug")) == WC2026_FIXTURE_SERIES_SLUG
        and _text(item.get("id")) is not None
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "Gamma series lookup did not resolve exactly one soccer-fifwc series"
        )
    return str(matches[0]["id"])


def _referenced_event_ids(events: Iterable[dict[str, Any]]) -> set[str]:
    referenced: set[str] = set()
    for event in events:
        for market in event.get("markets") or []:
            if not isinstance(market, dict):
                continue
            for related in market.get("events") or []:
                if isinstance(related, dict) and _text(related.get("id")):
                    referenced.add(str(related["id"]))
    return referenced


def _partition_inventory(
    events: dict[str, dict[str, Any]],
) -> tuple[tuple[Any, ...], int, int]:
    """Return the canonical membership inventory used for scan convergence.

    Event IDs alone are insufficient: Gamma can return the same events while a
    nested child-market page is still moving.  The effective related IDs include
    the enclosing event because that is the bridge fallback when Gamma omits a
    market's ``events`` array.
    """
    inventory: list[tuple[str, tuple[tuple[str, tuple[str, ...]], ...]]] = []
    child_market_count = 0
    membership_count = 0
    for event_id in sorted(events):
        markets: dict[str, set[str]] = {}
        for market in events[event_id].get("markets") or []:
            if not isinstance(market, dict):
                continue
            market_id = _text(market.get("id"))
            if market_id is None:
                continue
            related_ids = markets.setdefault(market_id, {event_id})
            for related in market.get("events") or []:
                if not isinstance(related, dict):
                    continue
                related_id = _text(related.get("id"))
                if related_id is not None:
                    related_ids.add(related_id)
        market_inventory = tuple(
            (market_id, tuple(sorted(related_ids)))
            for market_id, related_ids in sorted(markets.items())
        )
        child_market_count += len(market_inventory)
        membership_count += sum(len(related_ids) for _, related_ids in market_inventory)
        inventory.append((event_id, market_inventory))
    return tuple(inventory), child_market_count, membership_count


def _inventory_sha256(inventory: tuple[Any, ...]) -> str:
    encoded = json.dumps(inventory, separators=(",", ":"), ensure_ascii=True)
    return sha256(encoded.encode()).hexdigest()


def _payload_inventory_sha256(events: dict[str, dict[str, Any]]) -> str:
    """Bind each converged partition to the payload that feeds semantics."""
    encoded = json.dumps(
        {event_id: events[event_id] for event_id in sorted(events)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )
    return sha256(encoded.encode()).hexdigest()


def _merge_event_payloads(
    events: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = _text(event.get("id"))
        if event_id is None:
            continue
        previous = merged.get(event_id)
        if previous is None:
            merged[event_id] = event
            continue
        markets = {
            _text(market.get("id")): market
            for market in previous.get("markets") or []
            if isinstance(market, dict) and _text(market.get("id"))
        }
        markets.update(
            {
                _text(market.get("id")): market
                for market in event.get("markets") or []
                if isinstance(market, dict) and _text(market.get("id"))
            }
        )
        tags = {
            (_text(tag.get("id")) or _text(tag.get("slug"))): tag
            for tag in [*(previous.get("tags") or []), *(event.get("tags") or [])]
            if isinstance(tag, dict)
            and (_text(tag.get("id")) or _text(tag.get("slug")))
        }
        merged[event_id] = {
            **previous,
            **event,
            "markets": list(markets.values()),
            "tags": list(tags.values()),
        }
    return merged


def collect_wc2026_event_catalog(
    *,
    client: Any | None = None,
    observed_at: datetime | None = None,
    max_pages: int | None = None,
    event_tag: str = WC2026_EVENT_TAG,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    include_slug_prefix_recall: bool = False,
    slug_prefix_recall_max_pages_without_progress: int | None = None,
    load_checkpoint_fn: Callable[[], dict[str, dict[str, Any]]] | None = None,
    save_checkpoint_fn: (
        Callable[[str, dict[str, dict[str, Any]], dict[str, Any]], None] | None
    ) = None,
) -> EventCatalogBatch:
    """Collect audited WC2026 candidates across complete Gamma partitions."""
    http = client or build_client()
    captured_at = observed_at or datetime.now(timezone.utc)
    payloads: list[dict[str, Any]] = []
    candidate_sources: dict[str, set[str]] = {}
    source_endpoints: dict[str, str] = {}
    scan_partitions: dict[str, dict[str, Any]] = {}
    checkpoints = load_checkpoint_fn() if load_checkpoint_fn is not None else {}

    def _apply_partition_result(
        source: str,
        partition: str,
        stable_events: dict[str, dict[str, Any]],
        partition_summary: dict[str, Any],
    ) -> None:
        for event_id, event in stable_events.items():
            payloads.append(event)
            candidate_sources.setdefault(event_id, set()).add(source)
            source_endpoints[event_id] = "/events/keyset"
        scan_partitions[partition] = partition_summary

    def scan(
        source: str,
        *,
        tag_slug: str | None = None,
        series_id: str | None = None,
        related_tags: bool = False,
        event_slug_prefixes: tuple[str, ...] = (),
    ) -> None:
        for closed in (False, True):
            state = "closed" if closed else "open"
            partition = f"{source}:{state}"
            cached = checkpoints.get(partition)
            if isinstance(cached, dict):
                cached_events = cached.get("stable_events")
                cached_summary = cached.get("scan_summary")
                # Incomplete early-stop caches must be rescanned so exhaustive
                # recall audits can converge to all_scan_partitions_complete.
                if (
                    isinstance(cached_events, dict)
                    and isinstance(cached_summary, dict)
                    and cached_summary.get("complete") is True
                ):
                    _apply_partition_result(
                        source, partition, cached_events, cached_summary
                    )
                    continue
            previous_inventory: tuple[Any, ...] | None = None
            stable_events: dict[str, dict[str, Any]] | None = None
            attempt_metadata: list[dict[str, Any]] = []
            for attempt in range(1, SCAN_CONVERGENCE_ATTEMPTS + 1):
                pages = 0
                attempt_events: dict[str, dict[str, Any]] = {}
                pages_without_match = 0
                early_stopped = False
                for events, meta in iter_gamma_events_keyset(
                    http,
                    max_pages=max_pages,
                    keyset_closed=closed,
                    keyset_tag_slug=tag_slug,
                    keyset_series_id=series_id,
                    keyset_related_tags=related_tags,
                    # Admission is decided from event volume after discovery.
                    # Passing even a zero floor asks Gamma to omit unknown-volume
                    # rows, which would make that audit population invisible.
                    keyset_volume_min=None,
                    progress_callback=progress_callback,
                    progress_task=(
                        f"wc2026_event_catalog_{source}_{state}_attempt_{attempt}"
                    ),
                ):
                    pages = meta.pages_done
                    if meta.truncated:
                        raise RuntimeError(
                            f"WC2026 {partition} scan truncated after {pages} pages"
                        )
                    matched_this_page = 0
                    for event in events:
                        if not isinstance(event, dict):
                            continue
                        event_id = _text(event.get("id"))
                        if event_id is None:
                            continue
                        # related_tags expands Gamma page breadth only; local
                        # membership still requires tag / series / slug-prefix.
                        matches_source = (
                            (
                                tag_slug is not None
                                and tag_slug.lower() in _event_tag_slugs(event)
                            )
                            or (
                                series_id is not None
                                and WC2026_FIXTURE_SERIES_SLUG in _series_slugs(event)
                            )
                            or (
                                bool(event_slug_prefixes)
                                and any(
                                    str(event.get("slug") or "")
                                    .strip()
                                    .lower()
                                    .startswith(prefix)
                                    for prefix in event_slug_prefixes
                                )
                            )
                        )
                        if matches_source:
                            attempt_events[event_id] = event
                            matched_this_page += 1
                    if event_slug_prefixes:
                        if matched_this_page == 0:
                            pages_without_match += 1
                        else:
                            pages_without_match = 0
                        if (
                            slug_prefix_recall_max_pages_without_progress is not None
                            and pages_without_match
                            >= slug_prefix_recall_max_pages_without_progress
                        ):
                            early_stopped = True
                            break
                ids = frozenset(attempt_events)
                event_ids_signature = sha256(
                    "\n".join(sorted(ids)).encode()
                ).hexdigest()
                membership_inventory, child_market_count, membership_count = (
                    _partition_inventory(attempt_events)
                )
                membership_signature = _inventory_sha256(membership_inventory)
                payload_signature = _payload_inventory_sha256(attempt_events)
                attempt_metadata.append(
                    {
                        "attempt": attempt,
                        "pages": pages,
                        "event_count": len(ids),
                        "event_ids_sha256": event_ids_signature,
                        "child_market_count": child_market_count,
                        "membership_count": membership_count,
                        "membership_inventory_sha256": membership_signature,
                        "event_payload_inventory_sha256": payload_signature,
                        "early_stopped": early_stopped,
                    }
                )
                if previous_inventory == membership_inventory:
                    stable_events = attempt_events
                    break
                previous_inventory = membership_inventory
            if stable_events is None:
                raise RuntimeError(
                    f"WC2026 {partition} scan_unstable after "
                    f"{SCAN_CONVERGENCE_ATTEMPTS} complete attempts"
                )
            accepted_early_stopped = bool(attempt_metadata[-1].get("early_stopped"))
            partition_summary = {
                "attempts": attempt_metadata,
                "event_count": len(stable_events),
                "event_ids_sha256": attempt_metadata[-1]["event_ids_sha256"],
                "child_market_count": attempt_metadata[-1]["child_market_count"],
                "membership_count": attempt_metadata[-1]["membership_count"],
                "membership_inventory_sha256": attempt_metadata[-1][
                    "membership_inventory_sha256"
                ],
                "event_payload_inventory_sha256": attempt_metadata[-1][
                    "event_payload_inventory_sha256"
                ],
                "complete": not accepted_early_stopped,
                "early_stopped": accepted_early_stopped,
                "stable": True,
            }
            _apply_partition_result(source, partition, stable_events, partition_summary)
            if save_checkpoint_fn is not None:
                save_checkpoint_fn(partition, stable_events, partition_summary)

    scan("exact_2026_tag", tag_slug=event_tag)
    # Related-tag expansion is recall-only. Returned events remain subject to
    # the same explicit membership review as every non-fixture candidate.
    scan(
        "related_2026_tag_recall",
        tag_slug=event_tag,
        related_tags=True,
    )
    scan("broad_fifa_world_cup_tag", tag_slug=WC2026_RECALL_TAG)
    series_id = _fixture_series_id(http)
    scan("soccer_fifwc_series", series_id=series_id)
    # Gamma has no slug-prefix filter. Exhaustively scan both lifecycle states
    # and apply the audited prefixes locally so this recall path has the same
    # convergence/completeness proof as tag and series discovery. Routine jobs
    # skip this path; the dedicated recall-audit job keeps it exhaustive.
    if include_slug_prefix_recall:
        scan(
            "wc2026_event_slug_prefix_recall",
            event_slug_prefixes=WC2026_RECALL_EVENT_SLUG_PREFIXES,
        )

    events_by_id = _merge_event_payloads(payloads)
    unresolved = _referenced_event_ids(events_by_id.values()) - set(events_by_id)
    while unresolved:
        fetched: list[dict[str, Any]] = []
        for related_event_id in sorted(unresolved):
            event = fetch_gamma_event_by_id(http, related_event_id)
            if event is None:
                raise RuntimeError(
                    "WC2026 event-market catalog references missing Gamma event "
                    f"{related_event_id}"
                )
            fetched.append(event)
            candidate_sources.setdefault(related_event_id, set()).add(
                "market_membership_reference"
            )
            source_endpoints[related_event_id] = f"/events/{related_event_id}"
        payloads.extend(fetched)
        events_by_id = _merge_event_payloads(payloads)
        unresolved = _referenced_event_ids(events_by_id.values()) - set(events_by_id)

    event_rows: list[dict[str, Any]] = []
    tag_rows: list[dict[str, Any]] = []
    bridge_rows: list[dict[str, Any]] = []
    market_payloads: dict[str, dict[str, Any]] = {}
    for event_id in sorted(events_by_id):
        event = events_by_id[event_id]
        event_rows.append(
            _event_snapshot(
                event,
                captured_at,
                candidate_sources.get(event_id, {"market_membership_reference"}),
                source_endpoint=source_endpoints.get(event_id, f"/events/{event_id}"),
            )
        )
        tag_rows.extend(_tag_rows(event, captured_at))
        event_bridges, event_markets = _event_market_rows(event, captured_at)
        bridge_rows.extend(event_bridges)
        for market in event_markets:
            market_id = _text(market.get("id"))
            if market_id is not None:
                market_payloads[market_id] = _merge_market_payload(
                    market_payloads.get(market_id), market
                )

    bridge_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in bridge_rows:
        key = (str(row["event_id"]), str(row["market_id"]))
        previous = bridge_by_key.get(key)
        if previous is None:
            bridge_by_key[key] = dict(row)
            continue
        enclosing = bool(previous["is_enclosing_event"]) or bool(
            row["is_enclosing_event"]
        )
        if int(row["source_ordinal"]) < int(previous["source_ordinal"]):
            bridge_by_key[key] = {**row, "is_enclosing_event": enclosing}
        else:
            previous["is_enclosing_event"] = enclosing
    bridge_rows = [bridge_by_key[key] for key in sorted(bridge_by_key)]

    eligible_events = sum(
        (row["event_volume_usd_lifetime_reported"] or -1)
        >= POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD
        for row in event_rows
    )
    return EventCatalogBatch(
        event_snapshots=tuple(event_rows),
        event_tag_snapshots=tuple(tag_rows),
        event_market_snapshots=tuple(bridge_rows),
        market_payloads=tuple(market_payloads[key] for key in sorted(market_payloads)),
        summary={
            "event_tag": event_tag,
            "volume_scan_floor_usd": None,
            "event_min_lifetime_volume_usd": POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD,
            "events": len(event_rows),
            "eligible_events_as_observed": eligible_events,
            "event_tags": len(tag_rows),
            "event_markets": len(bridge_rows),
            "unique_markets": len(market_payloads),
            "fixture_series_slug": WC2026_FIXTURE_SERIES_SLUG,
            "fixture_series_id": series_id,
            "candidate_sources": {
                source: sum(source in values for values in candidate_sources.values())
                for source in sorted(
                    {value for values in candidate_sources.values() for value in values}
                )
            },
            "scan_partitions": scan_partitions,
            "all_scan_partitions_complete": all(
                item["complete"] for item in scan_partitions.values()
            ),
            "volume_unknown_events": sum(
                row["event_volume_usd_lifetime_reported"] is None for row in event_rows
            ),
            "observed_at": captured_at.isoformat(),
        },
    )


__all__ = [
    "EventCatalogBatch",
    "POLYMARKET_WC2026_EVENT_MIN_VOLUME_USD",
    "SCAN_CONVERGENCE_ATTEMPTS",
    "WC2026_EVENT_TAG",
    "WC2026_FIXTURE_SERIES_SLUG",
    "WC2026_RECALL_TAG",
    "WC2026_RECALL_EVENT_SLUG_PREFIXES",
    "collect_wc2026_event_catalog",
]
