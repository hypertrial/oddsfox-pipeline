"""WC2026 registry refresh entrypoints."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Iterable, Sequence

from oddsfox.ingestion.polymarket.gamma_events import fetch_gamma_event_by_slug
from oddsfox.storage.duckdb.metadata import (
    get_wc2026_discovery_fully_checked,
    set_wc2026_discovery_fully_checked,
)
from oddsfox.storage.duckdb.wc2026_registry import (
    RegistryRow,
    get_registry_market_ids,
    upsert_registry_rows,
)

from .config import Wc2026ScopeConfig, load_wc2026_config, scope_config_hash
from .gamma import (
    _chunk_market_ids,
    _fetch_markets_batch_resilient,
    _gamma_market_ids,
)
from .predicates import (
    Wc2026EventsScanResult,
    _resolve_keyset_closed,
    _resolve_keyset_volume_min,
)
from .scan import (
    DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    DISCOVERY_MODE_FULL_KEYSET,
    DISCOVERY_MODE_TARGETED,
    _collect_from_events,
    _collect_from_market_payloads,
    _empty_scan_result,
    _merge_scan_results,
    _scan_wc2026_gamma_events,
)

logger = logging.getLogger(__name__)

_TARGETED_MARKETS_BATCH_SIZE = 50


def _record_discovery_ledger(
    cfg: Wc2026ScopeConfig,
    *,
    discovery_mode: str,
    truncated: bool,
) -> None:
    config_hash = scope_config_hash(cfg)
    from oddsfox.storage.duckdb.metadata import (
        get_wc2026_discovery_scope_config_hash,
    )

    stored_hash = get_wc2026_discovery_scope_config_hash()
    if stored_hash and stored_hash != config_hash:
        set_wc2026_discovery_fully_checked(False, scope_config_hash=config_hash)
    if discovery_mode == DISCOVERY_MODE_FULL_KEYSET and not truncated:
        set_wc2026_discovery_fully_checked(True, scope_config_hash=config_hash)
    elif get_wc2026_discovery_fully_checked() and truncated:
        set_wc2026_discovery_fully_checked(False, scope_config_hash=config_hash)


def _seed_registry_rows(cfg: Wc2026ScopeConfig) -> list[RegistryRow]:
    return [
        RegistryRow(
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
    seen: dict[str, RegistryRow] = {}
    for row in rows:
        prev = seen.get(row.market_id)
        if prev is None or _source_priority(row.source) > _source_priority(prev.source):
            seen[row.market_id] = row
    return list(seen.values())


def _count_by_source(rows: Sequence[RegistryRow]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.source] = counts.get(row.source, 0) + 1
    return counts


def _finalize_registry_collect(
    scan: Wc2026EventsScanResult,
    cfg: Wc2026ScopeConfig,
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
    registry_summary = {
        "task": "refresh_wc2026_registry",
        "discovery_mode": discovery_mode,
        "events_pages": scan.pages_done,
        "truncated": scan.truncated,
        "registry_rows_upserted": saved,
        "discovered_event_slugs": list(scan.discovered_slugs),
        "by_source": _count_by_source(merged),
        "duration_seconds": round(time.monotonic() - t0, 3),
        "api_requests": total_api,
        "registry_refreshed": True,
    }
    if keyset_closed is not None:
        registry_summary["keyset_closed"] = keyset_closed
    effective_crawl_tags = (
        list(scan.crawl_tag_slugs)
        if scan.crawl_tag_slugs
        else (list(keyset_tag_slugs) if keyset_tag_slugs else [])
    )
    if effective_crawl_tags:
        registry_summary["keyset_tag_slugs"] = effective_crawl_tags
        registry_summary["crawl_tag_slugs"] = effective_crawl_tags
    if scan.scope_tag_slugs:
        registry_summary["scope_tag_slugs"] = list(scan.scope_tag_slugs)
    if scan.tag_sources:
        registry_summary["tag_sources"] = {
            slug: list(srcs) for slug, srcs in scan.tag_sources
        }
    if keyset_volume_min is not None:
        registry_summary["keyset_volume_min"] = keyset_volume_min
    markets = list(scan.raw_markets)
    collect_meta = {
        "discovery_mode": discovery_mode,
        "events_pages": scan.pages_done,
        "truncated": scan.truncated,
        "markets_collected": len(markets),
        "api_requests": total_api,
        "registry_refreshed": True,
    }
    if keyset_closed is not None:
        collect_meta["keyset_closed"] = keyset_closed
    if effective_crawl_tags:
        collect_meta["keyset_tag_slugs"] = effective_crawl_tags
        collect_meta["crawl_tag_slugs"] = effective_crawl_tags
    if scan.scope_tag_slugs:
        collect_meta["scope_tag_slugs"] = list(scan.scope_tag_slugs)
    if scan.tag_sources:
        collect_meta["tag_sources"] = {
            slug: list(srcs) for slug, srcs in scan.tag_sources
        }
    if keyset_volume_min is not None:
        collect_meta["keyset_volume_min"] = keyset_volume_min
    return registry_summary, markets, collect_meta


def refresh_registry_and_collect_markets_targeted(
    client: Any,
    *,
    config: Wc2026ScopeConfig | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[Dict[str, Any], list[dict[str, Any]], Dict[str, Any]]:
    """Targeted discovery: allowlisted slugs plus /markets by seed and registry IDs."""
    cfg = config or load_wc2026_config()
    t0 = time.monotonic()
    merged = _empty_scan_result()
    api_requests = 0

    for slug in cfg.event_slugs:
        event = fetch_gamma_event_by_slug(client, slug)
        api_requests += 1
        if progress_callback:
            try:
                progress_callback(
                    "wc2026_event_by_slug",
                    {"slug": slug, "found": event is not None},
                )
            except Exception:
                logger.debug("Ignoring slug progress callback failure", exc_info=True)
        if event:
            merged = _merge_scan_results(merged, _collect_from_events([event], cfg))

    allowlisted_ids = set(cfg.market_ids) | set(get_registry_market_ids())
    market_ids = _gamma_market_ids(allowlisted_ids)
    for batch_idx, chunk in enumerate(
        _chunk_market_ids(market_ids, _TARGETED_MARKETS_BATCH_SIZE), start=1
    ):
        payloads = _fetch_markets_batch_resilient(client, chunk, include_events=True)
        api_requests += 1
        if progress_callback:
            try:
                progress_callback(
                    "wc2026_markets_by_id",
                    {
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

    merged = Wc2026EventsScanResult(
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
    config: Wc2026ScopeConfig | None = None,
    max_pages: int | None = None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    """Scan Gamma /events/keyset and upsert WC 2026 markets into the ops registry."""
    cfg = config or load_wc2026_config()
    t0 = time.monotonic()
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    scan = _scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=effective_volume,
        tag_discovery=tag_discovery,
        progress_callback=progress_callback,
    )
    registry_summary, _, _ = _finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=DISCOVERY_MODE_FULL_KEYSET,
        t0=t0,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=keyset_volume_min,
    )
    return registry_summary


def collect_wc2026_markets_from_events(
    client: Any,
    *,
    config: Wc2026ScopeConfig | None = None,
    max_pages: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
) -> tuple[list[dict[str, Any]], Dict[str, Any]]:
    """Return raw Gamma market dicts for WC events (for event-first inventory sync)."""
    cfg = config or load_wc2026_config()
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    scan = _scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=max_pages,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=effective_volume,
        tag_discovery=tag_discovery,
        progress_task="wc2026_market_events",
    )
    markets = list(scan.raw_markets)
    meta = {
        "events_pages": scan.pages_done,
        "truncated": scan.truncated,
        "markets_collected": len(markets),
    }
    if effective_closed is not None:
        meta["keyset_closed"] = effective_closed
    if scan.crawl_tag_slugs:
        meta["keyset_tag_slugs"] = list(scan.crawl_tag_slugs)
        meta["crawl_tag_slugs"] = list(scan.crawl_tag_slugs)
    elif keyset_tag_slugs:
        meta["keyset_tag_slugs"] = list(keyset_tag_slugs)
    if scan.scope_tag_slugs:
        meta["scope_tag_slugs"] = list(scan.scope_tag_slugs)
    if scan.tag_sources:
        meta["tag_sources"] = {slug: list(srcs) for slug, srcs in scan.tag_sources}
    if effective_volume is not None:
        meta["keyset_volume_min"] = effective_volume
    return markets, meta


def refresh_registry_and_collect_markets_from_events(
    client: Any,
    *,
    config: Wc2026ScopeConfig | None = None,
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
    cfg = config or load_wc2026_config()
    t0 = time.monotonic()
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    scan = _scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=effective_volume,
        tag_discovery=tag_discovery,
        progress_callback=progress_callback,
        progress_task="wc2026_market_events",
    )
    return _finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=DISCOVERY_MODE_FULL_KEYSET,
        t0=t0,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=effective_volume,
    )
