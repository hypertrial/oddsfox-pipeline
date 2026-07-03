"""Market-scope event/market predicates and tag crawl rules."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

from oddsfox_pipeline.ingestion.polymarket.scope_sql import (
    DEFAULT_MARKET_SCOPE,
    MARKET_SCOPE_ALL,
    validate_market_scope,
)
from oddsfox_pipeline.storage.duckdb.market_scope_registry import RegistryRow

from .config import MarketScopeConfig, _validate_slug_token, load_market_scope_config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolvedMarketScopeDiscovery:
    """Effective event-scan options for one market-scope run."""

    keyset_closed: bool | None
    keyset_related_tags: bool
    keyset_volume_min: float | None
    keyset_tag_slugs: tuple[str, ...] | None
    tag_discovery: bool
    pass_page_cap: int | None
    total_page_budget: int | None
    max_pages_without_progress: int | None
    scope_tag_slugs: tuple[str, ...]
    scope_for_passes: tuple[str, ...] | None
    seed_tag_slugs: tuple[str, ...]
    max_closure_rounds: int
    max_crawl_tags: int | None


def resolve_market_scope_discovery(
    config: MarketScopeConfig,
    *,
    max_pages: int | None,
    max_pages_without_progress: int | None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
) -> ResolvedMarketScopeDiscovery:
    scope_tag_slugs = tuple(config.event_tags)
    cap = config.tag_crawl_max
    return ResolvedMarketScopeDiscovery(
        keyset_closed=_resolve_keyset_closed(keyset_closed, config),
        keyset_related_tags=_resolve_keyset_related_tags(keyset_related_tags, config),
        keyset_volume_min=_resolve_keyset_volume_min(keyset_volume_min, config),
        keyset_tag_slugs=(
            tuple(_validate_slug_token(slug) for slug in keyset_tag_slugs)
            if keyset_tag_slugs is not None
            else None
        ),
        tag_discovery=config.tag_discovery if tag_discovery is None else tag_discovery,
        pass_page_cap=max_pages
        if max_pages is not None
        else config.registry_max_event_pages,
        total_page_budget=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        scope_tag_slugs=scope_tag_slugs,
        scope_for_passes=scope_tag_slugs if scope_tag_slugs else None,
        seed_tag_slugs=tuple(config.default_keyset_tag_slugs),
        max_closure_rounds=max(0, config.tag_closure_rounds),
        max_crawl_tags=None if cap is None or cap <= 0 else cap,
    )


def _event_tag_slugs(event: dict[str, Any]) -> frozenset[str]:
    tags = event.get("tags")
    if not isinstance(tags, list):
        return frozenset()
    slugs: set[str] = set()
    for tag in tags:
        if isinstance(tag, dict):
            slug = (tag.get("slug") or "").strip().lower()
            if slug:
                slugs.add(slug)
    return frozenset(slugs)


def _resolve_keyset_closed(
    keyset_closed: bool | None,
    config: MarketScopeConfig | None = None,
) -> bool | None:
    if keyset_closed is not None:
        return keyset_closed
    return config.keyset_closed if config is not None else None


def _resolve_keyset_volume_min(
    keyset_volume_min: float | None,
    config: MarketScopeConfig | None = None,
) -> float | None:
    if keyset_volume_min is not None:
        return keyset_volume_min
    return config.keyset_volume_min if config is not None else None


def _resolve_keyset_related_tags(
    keyset_related_tags: bool | None,
    config: MarketScopeConfig | None = None,
) -> bool:
    if keyset_related_tags is not None:
        return keyset_related_tags
    return config.keyset_related_tags if config is not None else True


def _parse_tag_discovery_keywords(raw: str | None) -> tuple[str, ...]:
    if not raw or not str(raw).strip():
        from oddsfox_pipeline.ingestion.polymarket.market_scope_tags import (
            DEFAULT_MARKET_SCOPE_TAG_DISCOVERY_KEYWORDS,
        )

        return DEFAULT_MARKET_SCOPE_TAG_DISCOVERY_KEYWORDS
    return tuple(k.strip().lower() for k in str(raw).split(",") if k.strip())


def _crawl_tag_allowed(
    slug: str | None,
    *,
    scope_tags: Sequence[str],
    seed_tags: Sequence[str],
    denylist: Sequence[str] = (),
    keyword_gate: bool = True,
    keywords: Sequence[str] = (),
) -> bool:
    """True when a tag slug may be crawled via /events/keyset."""
    if slug is None:
        return True
    normalized = str(slug).strip().lower()
    if not normalized:
        return False

    scope_set = frozenset(str(t).strip().lower() for t in scope_tags if str(t).strip())
    seed_set = frozenset(str(t).strip().lower() for t in seed_tags if str(t).strip())
    if normalized in scope_set or normalized in seed_set:
        return True

    if normalized in frozenset(
        str(d).strip().lower() for d in denylist if str(d).strip()
    ):
        return False

    if not keyword_gate:
        return True

    from oddsfox_pipeline.ingestion.polymarket.market_scope_tags import (
        tag_matches_keywords,
    )

    effective_keywords = tuple(keywords) or _parse_tag_discovery_keywords(None)
    return tag_matches_keywords({"slug": normalized}, effective_keywords)


def _filter_crawl_tag_slugs(
    tag_slugs: Sequence[str],
    *,
    scope_tags: Sequence[str],
    seed_tags: Sequence[str],
    config: MarketScopeConfig | None = None,
) -> list[str]:
    cfg = config or load_market_scope_config()
    filtered: list[str] = []
    for slug in tag_slugs:
        if _crawl_tag_allowed(
            slug,
            scope_tags=scope_tags,
            seed_tags=seed_tags,
            denylist=cfg.tag_crawl_denylist,
            keyword_gate=cfg.tag_closure_keyword_gate,
            keywords=cfg.tag_discovery_keywords,
        ):
            filtered.append(slug)
        else:
            logger.info(
                "Skipping market-scope crawl tag %s (denylist or keyword gate)",
                slug,
            )
    return filtered


def event_matches_scope_tags(
    event: dict[str, Any] | None,
    *,
    config: MarketScopeConfig | None = None,
    scope_tag_slugs: Sequence[str] | None = None,
) -> bool:
    if not event or not isinstance(event, dict):
        return False
    cfg = config or load_market_scope_config()
    allowed_slugs = (
        tuple(scope_tag_slugs) if scope_tag_slugs is not None else cfg.event_tags
    )
    if not allowed_slugs:
        return False
    return bool(_event_tag_slugs(event) & frozenset(allowed_slugs))


def event_matches_scope_config(
    event_slug: str | None,
    *,
    config: MarketScopeConfig | None = None,
) -> bool:
    if not event_slug:
        return False
    cfg = config or load_market_scope_config()
    slug = event_slug.strip().lower()
    if slug in cfg.event_slugs:
        return True
    return any(slug.startswith(p) for p in cfg.event_slug_prefixes)


def event_in_scope(
    event: dict[str, Any] | None,
    *,
    config: MarketScopeConfig | None = None,
    keyset_tag_slug: str | None = None,
    keyset_related_tags: bool = False,
    scope_tag_slugs: Sequence[str] | None = None,
) -> bool:
    """True when a Gamma event belongs to the configured market scope."""
    if not event or not isinstance(event, dict):
        return False
    cfg = config or load_market_scope_config()
    if keyset_tag_slug is not None and not keyset_related_tags:
        normalized_tag = _validate_slug_token(keyset_tag_slug)
        allowlist = (
            frozenset(scope_tag_slugs)
            if scope_tag_slugs is not None
            else frozenset(cfg.event_tags)
        )
        if normalized_tag in allowlist:
            return True
        return event_matches_scope_tags(
            event, config=cfg, scope_tag_slugs=scope_tag_slugs
        )

    event_slug = (event.get("slug") or "").strip().lower()
    if event_slug and event_matches_scope_config(event_slug, config=cfg):
        return True
    return event_matches_scope_tags(event, config=cfg, scope_tag_slugs=scope_tag_slugs)


def resolve_keyset_crawl_tags(
    keyset_tag_slugs: Sequence[str] | None,
    *,
    config: MarketScopeConfig | None = None,
    client: Any = None,
    tag_discovery: bool | None = None,
) -> tuple[list[str], dict[str, set[str]]]:
    """Return crawl tag slugs for /events/keyset and their discovery sources."""
    if keyset_tag_slugs is not None:
        slugs = list(keyset_tag_slugs)
        return slugs, {slug: {"explicit"} for slug in slugs}
    cfg = config or load_market_scope_config()
    base = list(cfg.default_keyset_tag_slugs)
    sources: dict[str, set[str]] = {slug: {"seed"} for slug in base}
    discovery_enabled = cfg.tag_discovery if tag_discovery is None else tag_discovery
    if not discovery_enabled or client is None:
        return base, sources

    from oddsfox_pipeline.ingestion.polymarket.market_scope_tags import (
        discover_market_scope_tag_slugs,
    )

    try:
        discovered = discover_market_scope_tag_slugs(
            client,
            seed_slugs=base,
            keywords=cfg.tag_discovery_keywords,
        )
    except Exception:
        logger.warning(
            "Market-scope tag discovery failed; using configured event_tags only",
            exc_info=True,
        )
        return base, sources
    for slug, srcs in discovered.sources.items():
        sources.setdefault(slug, set()).update(srcs)
    merged = sorted({*base, *discovered.tag_slugs})
    if merged != base:
        logger.info(
            "Market-scope tag discovery expanded crawl tags from %s to %s",
            base,
            merged,
        )
    return merged, sources


def resolve_keyset_tag_slugs(
    keyset_tag_slugs: Sequence[str] | None,
    *,
    config: MarketScopeConfig | None = None,
    client: Any = None,
    tag_discovery: bool | None = None,
) -> list[str]:
    """Return tag slugs for /events/keyset passes."""
    crawl_tags, _sources = resolve_keyset_crawl_tags(
        keyset_tag_slugs,
        config=config,
        client=client,
        tag_discovery=tag_discovery,
    )
    return crawl_tags


def is_market_scope_row(
    *,
    market_id: str,
    question: str = "",
    category: str = "",
    description: str = "",
    slug: str = "",
    event_slug: str = "",
    event_tags: Sequence[str] = (),
    market_scope: str = DEFAULT_MARKET_SCOPE,
    config: MarketScopeConfig | None = None,
    in_registry: bool = False,
) -> bool:
    """Pure-Python scope check for unit tests and local predicates."""
    scope = validate_market_scope(market_scope)
    if scope == MARKET_SCOPE_ALL:
        return True

    cfg = config or load_market_scope_config(scope_name=scope)
    if in_registry or market_id in cfg.market_ids:
        return True
    es = (event_slug or "").lower()
    if es in cfg.event_slugs:
        return True
    if any(es.startswith(p) for p in cfg.event_slug_prefixes):
        return True
    if cfg.event_tags and event_tags:
        allowed = frozenset(cfg.event_tags)
        if allowed & {t.strip().lower() for t in event_tags if str(t).strip()}:
            return True
    return False


@dataclass(frozen=True)
class MarketScopeEventsScanResult:
    """Gamma event collection result for a configured market scope."""

    registry_rows: tuple[RegistryRow, ...]
    raw_markets: tuple[dict[str, Any], ...]
    pages_done: int
    truncated: bool
    discovered_slugs: tuple[str, ...]
    api_requests: int = 0
    harvested_tag_slugs: frozenset[str] = frozenset()
    crawl_tag_slugs: tuple[str, ...] = ()
    scope_tag_slugs: tuple[str, ...] = ()
    tag_sources: tuple[tuple[str, tuple[str, ...]], ...] = ()
