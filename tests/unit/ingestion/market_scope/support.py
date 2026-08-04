"""Shared helpers for WC2026 scope unit tests."""

from __future__ import annotations

from oddsfox_pipeline.ingestion.polymarket.market_scope import MarketScopeConfig


def slug_only_cfg(**kwargs) -> MarketScopeConfig:
    defaults = {
        "event_slugs": ("2026-fifa-world-cup-winner-595",),
        "event_slug_prefixes": ("2026-fifa-world-cup",),
        "market_ids": (),
        "registry_max_event_pages": None,
        "event_tags": (),
        "keyset_closed": False,
        "keyset_volume_min": None,
        "keyset_related_tags": False,
        "tag_discovery": False,
        "tag_closure_rounds": 0,
        "tag_crawl_max": 100,
    }
    defaults.update(kwargs)
    return MarketScopeConfig(**defaults)
