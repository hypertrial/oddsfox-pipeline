"""Market-scope registry refresh entrypoints."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Iterable, Sequence

from oddsfox_pipeline.ingestion.polymarket.gamma_events import fetch_gamma_event_by_slug
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    get_registry_market_ids,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.metadata import (
    get_market_scope_discovery_fully_checked,
    get_market_scope_discovery_scope_config_hash,
    set_market_scope_discovery_fully_checked,
)

from .config import MarketScopeConfig, load_market_scope_config, scope_config_hash
from .gamma import (
    _chunk_market_ids,
    _fetch_markets_batch_resilient,
    _gamma_market_ids,
)
from .predicates import (
    MarketScopeEventsScanResult,
    ResolvedMarketScopeDiscovery,
    resolve_market_scope_discovery,
)
from .scan import (
    DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    DISCOVERY_MODE_FULL_KEYSET,
    DISCOVERY_MODE_TARGETED,
    _collect_from_events,
    _collect_from_market_payloads,
    _empty_scan_result,
    _merge_scan_results,
    _scan_market_scope_gamma_events,
)

logger = logging.getLogger(__name__)

_TARGETED_MARKETS_BATCH_SIZE = 50


def _record_discovery_ledger(
    cfg: MarketScopeConfig,
    *,
    discovery_mode: str,
    truncated: bool,
) -> None:
    config_hash = scope_config_hash(cfg)
    stored_hash = get_market_scope_discovery_scope_config_hash(cfg.scope_name)
    if stored_hash and stored_hash != config_hash:
        set_market_scope_discovery_fully_checked(
            cfg.scope_name,
            False,
            scope_config_hash=config_hash,
        )
    if discovery_mode == DISCOVERY_MODE_FULL_KEYSET and not truncated:
        set_market_scope_discovery_fully_checked(
            cfg.scope_name,
            True,
            scope_config_hash=config_hash,
        )
    elif get_market_scope_discovery_fully_checked(cfg.scope_name) and truncated:
        set_market_scope_discovery_fully_checked(
            cfg.scope_name,
            False,
            scope_config_hash=config_hash,
        )


def _seed_registry_rows(cfg: MarketScopeConfig) -> list[RegistryRow]:
    return [
        RegistryRow(
            scope_name=cfg.scope_name,
            market_id=market_id,
            event_slug=None,
            event_id=None,
            source="seed",
        )
        for market_id in cfg.market_ids
    ]


def _source_priority(source: str) -> int:
    return {"seed": 1, "events_api": 2, "markets_api": 2}.get(source, 0)


def _dedupe_registry_rows(rows: Iterable[RegistryRow]) -> list[RegistryRow]:
    seen: dict[tuple[str, str], RegistryRow] = {}
    for row in rows:
        key = (row.scope_name, row.market_id)
        prev = seen.get(key)
        if prev is None or _source_priority(row.source) > _source_priority(prev.source):
            seen[key] = row
    return list(seen.values())


def _count_by_source(rows: Sequence[RegistryRow]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.source] = counts.get(row.source, 0) + 1
    return counts


def _finalize_registry_collect(
    scan: MarketScopeEventsScanResult,
    cfg: MarketScopeConfig,
    *,
    discovery_mode: str,
    t0: float,
    api_requests: int = 0,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_volume_min: float | None = None,
) -> tuple[Dict[str, Any], list[dict[str, Any]], Dict[str, Any]]:
    _record_discovery_ledger(
        cfg,
        discovery_mode=discovery_mode,
        truncated=scan.truncated,
    )
    merged = _dedupe_registry_rows(list(scan.registry_rows) + _seed_registry_rows(cfg))
    saved = upsert_registry_rows(merged)
    total_api = scan.api_requests + api_requests
    markets = list(scan.raw_markets)
    scan_meta = _scan_collect_metadata(
        scan,
        cfg,
        discovery_mode=discovery_mode,
        total_api=total_api,
        markets_collected=len(markets),
        registry_refreshed=True,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=keyset_volume_min,
    )
    registry_summary = {
        "task": "refresh_market_scope_registry",
        "registry_rows_upserted": saved,
        "discovered_event_slugs": list(scan.discovered_slugs),
        "by_source": _count_by_source(merged),
        "duration_seconds": round(time.monotonic() - t0, 3),
    }
    registry_summary.update(scan_meta)
    return registry_summary, markets, scan_meta


def _scan_collect_metadata(
    scan: MarketScopeEventsScanResult,
    cfg: MarketScopeConfig,
    *,
    discovery_mode: str | None = None,
    total_api: int | None = None,
    markets_collected: int,
    registry_refreshed: bool | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_volume_min: float | None = None,
    include_fallback_crawl_tag_slugs: bool = True,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "events_pages": scan.pages_done,
        "truncated": scan.truncated,
        "markets_collected": markets_collected,
        "scope_name": cfg.scope_name,
    }
    if discovery_mode is not None:
        meta["discovery_mode"] = discovery_mode
    if total_api is not None:
        meta["api_requests"] = total_api
    if registry_refreshed is not None:
        meta["registry_refreshed"] = registry_refreshed
    if keyset_closed is not None:
        meta["keyset_closed"] = keyset_closed
    if scan.crawl_tag_slugs:
        crawl_tags = list(scan.crawl_tag_slugs)
        meta["keyset_tag_slugs"] = crawl_tags
        meta["crawl_tag_slugs"] = crawl_tags
    elif keyset_tag_slugs:
        fallback_tags = list(keyset_tag_slugs)
        meta["keyset_tag_slugs"] = fallback_tags
        if include_fallback_crawl_tag_slugs:
            meta["crawl_tag_slugs"] = fallback_tags
    if scan.scope_tag_slugs:
        meta["scope_tag_slugs"] = list(scan.scope_tag_slugs)
    if scan.tag_sources:
        meta["tag_sources"] = {slug: list(srcs) for slug, srcs in scan.tag_sources}
    if keyset_volume_min is not None:
        meta["keyset_volume_min"] = keyset_volume_min
    return meta


def _resolved_discovery(
    cfg: MarketScopeConfig,
    *,
    max_pages: int | None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
) -> ResolvedMarketScopeDiscovery:
    return resolve_market_scope_discovery(
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
        tag_discovery=tag_discovery,
    )


def _scan_events_for_scope(
    client: Any,
    *,
    config: MarketScopeConfig | None = None,
    max_pages: int | None = None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_task: str = "market_scope_registry_events",
) -> tuple[
    MarketScopeConfig,
    ResolvedMarketScopeDiscovery,
    MarketScopeEventsScanResult,
]:
    cfg = config or load_market_scope_config()
    resolved = _resolved_discovery(
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
        tag_discovery=tag_discovery,
    )
    scan = _scan_market_scope_gamma_events(
        client,
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
        tag_discovery=tag_discovery,
        progress_callback=progress_callback,
        progress_task=progress_task,
        resolved=resolved,
    )
    return cfg, resolved, scan


def refresh_registry_and_collect_markets_targeted(
    client: Any,
    *,
    config: MarketScopeConfig | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[Dict[str, Any], list[dict[str, Any]], Dict[str, Any]]:
    """Targeted discovery: allowlisted slugs plus /markets by seed and registry IDs."""
    cfg = config or load_market_scope_config()
    t0 = time.monotonic()
    merged = _empty_scan_result()
    api_requests = 0

    for slug in cfg.event_slugs:
        event = fetch_gamma_event_by_slug(client, slug)
        api_requests += 1
        if progress_callback:
            try:
                progress_callback(
                    "market_scope_event_by_slug",
                    {
                        "scope_name": cfg.scope_name,
                        "slug": slug,
                        "found": event is not None,
                    },
                )
            except Exception:
                logger.debug("Ignoring slug progress callback failure", exc_info=True)
        if event:
            merged = _merge_scan_results(merged, _collect_from_events([event], cfg))

    allowlisted_ids = set(cfg.market_ids) | set(get_registry_market_ids(cfg.scope_name))
    market_ids = _gamma_market_ids(allowlisted_ids)
    for batch_idx, chunk in enumerate(
        _chunk_market_ids(market_ids, _TARGETED_MARKETS_BATCH_SIZE), start=1
    ):
        payloads = _fetch_markets_batch_resilient(client, chunk, include_events=True)
        api_requests += 1
        if progress_callback:
            try:
                progress_callback(
                    "market_scope_markets_by_id",
                    {
                        "scope_name": cfg.scope_name,
                        "batch": batch_idx,
                        "chunk_size": len(chunk),
                        "markets_fetched": len(payloads or []),
                    },
                )
            except Exception:
                logger.debug(
                    "Ignoring markets-by-id progress callback failure", exc_info=True
                )
        merged = _merge_scan_results(
            merged,
            _collect_from_market_payloads(
                payloads or [], cfg, allowlisted_market_ids=allowlisted_ids
            ),
        )

    merged = MarketScopeEventsScanResult(
        registry_rows=merged.registry_rows,
        raw_markets=merged.raw_markets,
        pages_done=0,
        truncated=False,
        discovered_slugs=merged.discovered_slugs,
        api_requests=api_requests,
    )
    return _finalize_registry_collect(
        merged,
        cfg,
        discovery_mode=DISCOVERY_MODE_TARGETED,
        t0=t0,
    )


def refresh_registry_from_events(
    client: Any,
    *,
    config: MarketScopeConfig | None = None,
    max_pages: int | None = None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    """Scan Gamma /events/keyset and upsert markets into the scope registry."""
    t0 = time.monotonic()
    cfg, resolved, scan = _scan_events_for_scope(
        client,
        config=config,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
        tag_discovery=tag_discovery,
        progress_callback=progress_callback,
    )
    registry_summary, _, _ = _finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=DISCOVERY_MODE_FULL_KEYSET,
        t0=t0,
        keyset_closed=resolved.keyset_closed,
        keyset_tag_slugs=resolved.keyset_tag_slugs,
        keyset_volume_min=resolved.keyset_volume_min,
    )
    return registry_summary


def collect_scope_markets_from_events(
    client: Any,
    *,
    config: MarketScopeConfig | None = None,
    max_pages: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
) -> tuple[list[dict[str, Any]], Dict[str, Any]]:
    """Return raw Gamma market dicts for WC2026 events."""
    cfg, resolved, scan = _scan_events_for_scope(
        client,
        config=config,
        max_pages=max_pages,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
        tag_discovery=tag_discovery,
        progress_task="market_scope_market_events",
    )
    markets = list(scan.raw_markets)
    meta = _scan_collect_metadata(
        scan,
        cfg,
        markets_collected=len(markets),
        keyset_closed=resolved.keyset_closed,
        keyset_tag_slugs=resolved.keyset_tag_slugs,
        keyset_volume_min=resolved.keyset_volume_min,
        include_fallback_crawl_tag_slugs=False,
    )
    return markets, meta


def refresh_registry_and_collect_markets_from_events(
    client: Any,
    *,
    config: MarketScopeConfig | None = None,
    max_pages: int | None = None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[Dict[str, Any], list[dict[str, Any]], Dict[str, Any]]:
    """Single /events pass: upsert registry and return raw markets for event-first sync."""
    t0 = time.monotonic()
    cfg, resolved, scan = _scan_events_for_scope(
        client,
        config=config,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=keyset_volume_min,
        tag_discovery=tag_discovery,
        progress_callback=progress_callback,
        progress_task="market_scope_market_events",
    )
    return _finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=DISCOVERY_MODE_FULL_KEYSET,
        t0=t0,
        keyset_closed=resolved.keyset_closed,
        keyset_tag_slugs=resolved.keyset_tag_slugs,
        keyset_volume_min=resolved.keyset_volume_min,
    )
