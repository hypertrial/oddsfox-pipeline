"""Polymarket market-scope presets, discovery, registry refresh, and SQL filters."""

from __future__ import annotations

from oddsfox_pipeline.ingestion.polymarket.scope_sql import (
    DEFAULT_MARKET_SCOPE,
    market_scope_predicate_sql,
    market_scope_sql,
    validate_market_scope,
)

from .config import (
    MarketScopeConfig,
    default_market_scopes_seed_path,
    load_market_scope_config,
    scope_config_hash,
)
from .predicates import (
    MarketScopeEventsScanResult,
    ResolvedMarketScopeDiscovery,
    _crawl_tag_allowed,
    event_in_scope,
    event_matches_scope_config,
    event_matches_scope_tags,
    is_market_scope_row,
    resolve_keyset_crawl_tags,
    resolve_keyset_tag_slugs,
    resolve_market_scope_discovery,
)
from .registry import (
    collect_markets_from_registry,
    collect_scope_markets_from_events,
    refresh_registry_and_collect_markets_from_events,
    refresh_registry_and_collect_markets_targeted,
    refresh_registry_from_event_catalog,
    refresh_registry_from_events,
)
from .scan import (
    DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    DISCOVERY_MODE_FULL_KEYSET,
    DISCOVERY_MODE_TARGETED,
    DiscoveryMode,
    _scan_market_scope_gamma_events,
)

__all__ = [
    "DEFAULT_MARKET_SCOPE",
    "DEFAULT_MAX_PAGES_WITHOUT_PROGRESS",
    "DISCOVERY_MODE_FULL_KEYSET",
    "DISCOVERY_MODE_TARGETED",
    "DiscoveryMode",
    "MarketScopeConfig",
    "MarketScopeEventsScanResult",
    "ResolvedMarketScopeDiscovery",
    "_crawl_tag_allowed",
    "_scan_market_scope_gamma_events",
    "collect_markets_from_registry",
    "collect_scope_markets_from_events",
    "default_market_scopes_seed_path",
    "event_in_scope",
    "event_matches_scope_config",
    "event_matches_scope_tags",
    "is_market_scope_row",
    "load_market_scope_config",
    "market_scope_predicate_sql",
    "market_scope_sql",
    "refresh_registry_and_collect_markets_from_events",
    "refresh_registry_and_collect_markets_targeted",
    "refresh_registry_from_event_catalog",
    "refresh_registry_from_events",
    "resolve_market_scope_discovery",
    "resolve_keyset_crawl_tags",
    "resolve_keyset_tag_slugs",
    "scope_config_hash",
    "validate_market_scope",
]
