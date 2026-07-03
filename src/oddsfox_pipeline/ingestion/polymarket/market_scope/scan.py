"""Gamma event scanning for configured market scopes."""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterable, Iterator, Literal, Sequence

from oddsfox_pipeline.ingestion.polymarket.gamma_events import (
    EventsPageMeta,
    iter_gamma_events_keyset,
)
from oddsfox_pipeline.storage.duckdb.market_scope_registry import RegistryRow

from .config import MarketScopeConfig
from .predicates import (
    MarketScopeEventsScanResult,
    ResolvedMarketScopeDiscovery,
    _crawl_tag_allowed,
    _event_tag_slugs,
    _filter_crawl_tag_slugs,
    event_in_scope,
    event_matches_scope_config,
    resolve_market_scope_discovery,
)

logger = logging.getLogger(__name__)

DISCOVERY_MODE_TARGETED = "targeted"
DISCOVERY_MODE_FULL_KEYSET = "full_keyset"
DiscoveryMode = Literal["targeted", "full_keyset"]
DEFAULT_MAX_PAGES_WITHOUT_PROGRESS = 25
_EVENTS_PAGE_MARKER: object = object()


def _event_slug_from_market(market: dict[str, Any]) -> tuple[str | None, str | None]:
    events = market.get("events")
    if not isinstance(events, list) or not events:
        return None, None
    first = events[0]
    if not isinstance(first, dict):
        return None, None
    slug = (first.get("slug") or "").strip().lower() or None
    event_id = str(first.get("id") or "") or None
    return slug, event_id


def _collect_from_events(
    events: Iterable[dict[str, Any]],
    cfg: MarketScopeConfig,
    *,
    source: str = "events_api",
    require_allowlist_match: bool = True,
    keyset_tag_slug: str | None = None,
    keyset_related_tags: bool = False,
    scope_tag_slugs: Sequence[str] | None = None,
) -> MarketScopeEventsScanResult:
    rows: list[RegistryRow] = []
    markets: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    discovered_slugs: set[str] = set()

    for event in events:
        event_slug = (event.get("slug") or "").strip().lower()
        if not event_slug:
            continue
        if require_allowlist_match and not event_in_scope(
            event,
            config=cfg,
            keyset_tag_slug=keyset_tag_slug,
            keyset_related_tags=keyset_related_tags,
            scope_tag_slugs=scope_tag_slugs,
        ):
            continue
        discovered_slugs.add(event_slug)
        event_id = str(event.get("id") or "") or None
        for market in event.get("markets") or []:
            if not isinstance(market, dict):
                continue
            market_id = str(market.get("id") or "").strip()
            if not market_id:
                continue
            rows.append(
                RegistryRow(
                    scope_name=cfg.scope_name,
                    market_id=market_id,
                    event_slug=event_slug,
                    event_id=event_id,
                    source=source,
                )
            )
            if market_id in seen_market_ids:
                continue
            seen_market_ids.add(market_id)
            if not market.get("events"):
                market = {
                    **market,
                    "events": [{"slug": event_slug, "id": event.get("id")}],
                }
            markets.append(market)

    return MarketScopeEventsScanResult(
        registry_rows=tuple(rows),
        raw_markets=tuple(markets),
        pages_done=0,
        truncated=False,
        discovered_slugs=tuple(sorted(discovered_slugs)),
    )


def _collect_from_market_payloads(
    market_payloads: Iterable[dict[str, Any]],
    cfg: MarketScopeConfig,
    *,
    allowlisted_market_ids: set[str],
) -> MarketScopeEventsScanResult:
    rows: list[RegistryRow] = []
    markets: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    discovered_slugs: set[str] = set()

    for market in market_payloads:
        if not isinstance(market, dict):
            continue
        market_id = str(market.get("id") or "").strip()
        if not market_id:
            continue
        event_slug, event_id = _event_slug_from_market(market)
        in_allowlist = market_id in allowlisted_market_ids
        slug_match = event_slug and event_matches_scope_config(event_slug, config=cfg)
        if not in_allowlist and not slug_match:
            continue
        if event_slug:
            discovered_slugs.add(event_slug)
        rows.append(
            RegistryRow(
                scope_name=cfg.scope_name,
                market_id=market_id,
                event_slug=event_slug,
                event_id=event_id,
                source="markets_api",
            )
        )
        if market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        markets.append(market)

    return MarketScopeEventsScanResult(
        registry_rows=tuple(rows),
        raw_markets=tuple(markets),
        pages_done=0,
        truncated=False,
        discovered_slugs=tuple(sorted(discovered_slugs)),
    )


def _merge_scan_results(
    left: MarketScopeEventsScanResult,
    right: MarketScopeEventsScanResult,
) -> MarketScopeEventsScanResult:
    rows = list(left.registry_rows) + list(right.registry_rows)
    markets_by_id: dict[str, dict[str, Any]] = {
        str(m.get("id")): m for m in left.raw_markets if m.get("id")
    }
    for market in right.raw_markets:
        market_id = str(market.get("id") or "")
        if market_id:
            markets_by_id[market_id] = market
    slugs = set(left.discovered_slugs) | set(right.discovered_slugs)
    harvested = left.harvested_tag_slugs | right.harvested_tag_slugs
    return MarketScopeEventsScanResult(
        registry_rows=tuple(rows),
        raw_markets=tuple(markets_by_id.values()),
        pages_done=max(left.pages_done, right.pages_done),
        truncated=left.truncated or right.truncated,
        discovered_slugs=tuple(sorted(slugs)),
        api_requests=left.api_requests + right.api_requests,
        harvested_tag_slugs=harvested,
        crawl_tag_slugs=left.crawl_tag_slugs or right.crawl_tag_slugs,
        scope_tag_slugs=left.scope_tag_slugs or right.scope_tag_slugs,
        tag_sources=left.tag_sources or right.tag_sources,
    )


def _empty_scan_result() -> MarketScopeEventsScanResult:
    return MarketScopeEventsScanResult(
        registry_rows=(),
        raw_markets=(),
        pages_done=0,
        truncated=False,
        discovered_slugs=(),
        api_requests=0,
    )


def _iter_market_scope_gamma_events(
    client: Any,
    cfg: MarketScopeConfig,
    *,
    max_pages: int | None,
    keyset_closed: bool | None = None,
    keyset_tag_slug: str | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    scope_tag_slugs: Sequence[str] | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_task: str = "market_scope_registry_events",
) -> Iterator[tuple[dict[str, Any], str, EventsPageMeta]]:
    """Yield (event_dict, normalized_event_slug, page_meta) for scoped events."""
    resolved = resolve_market_scope_discovery(
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=None,
        keyset_closed=keyset_closed,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
    )
    for events, page_meta in iter_gamma_events_keyset(
        client,
        max_pages=resolved.pass_page_cap,
        keyset_closed=resolved.keyset_closed,
        keyset_tag_slug=keyset_tag_slug,
        keyset_related_tags=resolved.keyset_related_tags,
        keyset_volume_min=resolved.keyset_volume_min,
        progress_callback=progress_callback,
        progress_task=progress_task,
    ):
        if not events:
            break
        for event in events:
            event_slug = (event.get("slug") or "").strip().lower()
            if not event_slug or not event_in_scope(
                event,
                config=cfg,
                keyset_tag_slug=keyset_tag_slug,
                keyset_related_tags=resolved.keyset_related_tags,
                scope_tag_slugs=scope_tag_slugs,
            ):
                continue
            yield event, event_slug, page_meta
        yield _EVENTS_PAGE_MARKER, "", page_meta


def _scan_market_scope_gamma_events_keyset_pass(
    client: Any,
    cfg: MarketScopeConfig,
    *,
    resolved: ResolvedMarketScopeDiscovery,
    page_cap: int | None,
    keyset_tag_slug: str | None,
    scope_tag_slugs: Sequence[str] | None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None,
    progress_task: str,
) -> MarketScopeEventsScanResult:
    """Single /events/keyset pass with optional tag and volume filters."""
    merged = _empty_scan_result()
    pages_done = 0
    truncated = False
    pages_without_progress = 0
    harvested_tag_slugs: set[str] = set()

    for events, page_meta in iter_gamma_events_keyset(
        client,
        max_pages=page_cap,
        keyset_closed=resolved.keyset_closed,
        keyset_tag_slug=keyset_tag_slug,
        keyset_related_tags=resolved.keyset_related_tags,
        keyset_volume_min=resolved.keyset_volume_min,
        progress_callback=progress_callback,
        progress_task=progress_task,
    ):
        pages_done = page_meta.pages_done
        truncated = page_meta.truncated
        if not events:
            break

        page_events = [
            event
            for event in events
            if (event.get("slug") or "").strip().lower()
            and event_in_scope(
                event,
                config=cfg,
                keyset_tag_slug=keyset_tag_slug,
                keyset_related_tags=resolved.keyset_related_tags,
                scope_tag_slugs=scope_tag_slugs,
            )
        ]
        if page_events:
            for event in page_events:
                harvested_tag_slugs.update(_event_tag_slugs(event))
            page_scan = _collect_from_events(
                page_events,
                cfg,
                keyset_tag_slug=keyset_tag_slug,
                keyset_related_tags=resolved.keyset_related_tags,
                scope_tag_slugs=scope_tag_slugs,
            )
            merged = _merge_scan_results(merged, page_scan)
            pages_without_progress = 0
        else:
            pages_without_progress += 1

        if (
            resolved.max_pages_without_progress is not None
            and pages_without_progress >= resolved.max_pages_without_progress
        ):
            truncated = True
            logger.info(
                "Stopping market-scope /events scan after %s pages without matches",
                pages_without_progress,
            )
            break

        if not events or page_meta.truncated:
            break

    return MarketScopeEventsScanResult(
        registry_rows=merged.registry_rows,
        raw_markets=merged.raw_markets,
        pages_done=pages_done,
        truncated=truncated,
        discovered_slugs=merged.discovered_slugs,
        api_requests=pages_done,
        harvested_tag_slugs=frozenset(harvested_tag_slugs),
    )


def _tag_crawl_key(tag_slug: str | None) -> str:
    return tag_slug if tag_slug is not None else "__all__"


def _queue_harvested_crawl_tags(
    harvested_tag_slugs: Iterable[str | None],
    *,
    crawled_set: set[str],
    next_queue: list[str | None],
    scope_tag_slugs: Sequence[str],
    seed_tag_slugs: Sequence[str],
    tag_sources_map: dict[str, set[str]],
    cfg: MarketScopeConfig,
) -> None:
    for harvested_slug in harvested_tag_slugs:
        if (
            harvested_slug is None
            or harvested_slug in crawled_set
            or harvested_slug in next_queue
        ):
            continue
        if not _crawl_tag_allowed(
            harvested_slug,
            scope_tags=scope_tag_slugs,
            seed_tags=seed_tag_slugs,
            denylist=cfg.tag_crawl_denylist,
            keyword_gate=cfg.tag_closure_keyword_gate,
            keywords=cfg.tag_discovery_keywords,
        ):
            continue
        next_queue.append(harvested_slug)
        tag_sources_map.setdefault(harvested_slug, set()).add("event_closure")


def _mark_crawled(
    crawl_key: str,
    *,
    crawled_set: set[str],
    crawled_keys: list[str],
) -> bool:
    if crawl_key in crawled_set:
        return False
    crawled_set.add(crawl_key)
    crawled_keys.append(crawl_key)
    return True


def _crawl_cap_reached(crawled_count: int, max_crawl_tags: int | None) -> bool:
    return max_crawl_tags is not None and crawled_count >= max_crawl_tags


def _remaining_page_budget(max_pages: int | None, total_pages: int) -> int | None:
    if max_pages is None:
        return None
    return max_pages - total_pages


def _scan_market_scope_gamma_events(
    client: Any,
    cfg: MarketScopeConfig,
    *,
    max_pages: int | None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_task: str = "market_scope_registry_events",
    resolved: ResolvedMarketScopeDiscovery | None = None,
) -> MarketScopeEventsScanResult:
    """One or more /events/keyset passes for the configured market scope."""
    resolved = resolved or resolve_market_scope_discovery(
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
        tag_discovery=tag_discovery,
    )
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        resolve_keyset_crawl_tags as _resolve_keyset_crawl_tags,
    )

    initial_crawl_tags, tag_sources_map = _resolve_keyset_crawl_tags(
        resolved.keyset_tag_slugs,
        config=cfg,
        client=client,
        tag_discovery=resolved.tag_discovery,
    )
    initial_crawl_tags = _filter_crawl_tag_slugs(
        initial_crawl_tags,
        scope_tags=resolved.scope_tag_slugs,
        seed_tags=resolved.seed_tag_slugs,
        config=cfg,
    )
    logger.info(
        "Market-scope tag crawl anchored to %s; initial crawl tags %s",
        list(resolved.scope_tag_slugs),
        initial_crawl_tags,
    )

    crawled_keys: list[str] = []
    crawled_set: set[str] = set()
    if initial_crawl_tags:
        queue: list[str | None] = list(dict.fromkeys(initial_crawl_tags))
    else:
        queue = [None]
    merged = _empty_scan_result()
    total_pages = 0
    truncated = False

    for closure_round in range(resolved.max_closure_rounds + 1):
        if not queue:
            break
        next_queue: list[str | None] = []
        for tag_slug in queue:
            crawl_key = _tag_crawl_key(tag_slug)
            if crawl_key in crawled_set:
                continue
            if _crawl_cap_reached(len(crawled_set), resolved.max_crawl_tags):
                truncated = True
                logger.info(
                    "Market-scope tag crawl cap reached (%s tags); stopping expansion",
                    resolved.max_crawl_tags,
                )
                break

            remaining_pages = _remaining_page_budget(
                resolved.total_page_budget, total_pages
            )
            if remaining_pages is not None and remaining_pages <= 0:
                truncated = True
                break

            _mark_crawled(
                crawl_key,
                crawled_set=crawled_set,
                crawled_keys=crawled_keys,
            )
            pass_scan = _scan_market_scope_gamma_events_keyset_pass(
                client,
                cfg,
                resolved=resolved,
                page_cap=(
                    remaining_pages
                    if remaining_pages is not None
                    else resolved.pass_page_cap
                ),
                keyset_tag_slug=tag_slug,
                scope_tag_slugs=resolved.scope_for_passes,
                progress_callback=progress_callback,
                progress_task=progress_task,
            )
            merged = _merge_scan_results(merged, pass_scan)
            total_pages += pass_scan.pages_done

            _queue_harvested_crawl_tags(
                pass_scan.harvested_tag_slugs,
                crawled_set=crawled_set,
                next_queue=next_queue,
                scope_tag_slugs=resolved.scope_tag_slugs,
                seed_tag_slugs=resolved.seed_tag_slugs,
                tag_sources_map=tag_sources_map,
                cfg=cfg,
            )

            if pass_scan.truncated:
                truncated = True
                break

        if truncated:
            break
        queue = list(dict.fromkeys(next_queue))

    final_crawl = tuple(slug for slug in crawled_keys if slug != "__all__")
    final_sources = tuple(
        (slug, tuple(sorted(tag_sources_map.get(slug, ("unknown",)))))
        for slug in final_crawl
    )
    logger.info(
        "Market-scope tag crawl complete: %s tags (scope anchored to %s)",
        len(final_crawl),
        list(resolved.scope_tag_slugs),
    )
    if final_sources:
        logger.info("Market-scope tag crawl sources: %s", dict(final_sources))

    return MarketScopeEventsScanResult(
        registry_rows=merged.registry_rows,
        raw_markets=merged.raw_markets,
        pages_done=total_pages,
        truncated=truncated,
        discovered_slugs=merged.discovered_slugs,
        api_requests=total_pages,
        crawl_tag_slugs=final_crawl,
        scope_tag_slugs=resolved.scope_tag_slugs,
        tag_sources=final_sources,
    )
