"""Shared helpers for WC2026 scope unit tests."""

from __future__ import annotations

import pytest

from oddsfox_pipeline.config import settings as config_settings
from oddsfox_pipeline.ingestion.polymarket.market_scope import MarketScopeConfig
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    predicates as scope_predicates_mod,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    scan as scope_scan_mod,
)


def slug_only_cfg(**kwargs) -> MarketScopeConfig:
    defaults = {
        "event_slugs": ("2026-fifa-world-cup-winner-595",),
        "event_slug_prefixes": ("2026-fifa-world-cup",),
        "market_ids": (),
        "registry_max_event_pages": None,
        "event_tags": (),
        "keyset_closed": False,
        "keyset_volume_min": 10000.0,
        "keyset_related_tags": False,
        "tag_discovery": False,
        "tag_closure_rounds": 0,
        "tag_crawl_max": 100,
    }
    defaults.update(kwargs)
    return MarketScopeConfig(**defaults)


@pytest.fixture(autouse=True)
def _market_scope_test_discovery_settings(monkeypatch):
    """Keep unit tests deterministic (no live Gamma tag discovery)."""
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_SCOPE_TAG_DISCOVERY", False, raising=False
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_SCOPE_TAG_CLOSURE_ROUNDS", 0, raising=False
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_SCOPE_TAG_CRAWL_MAX", 100, raising=False
    )
    monkeypatch.setattr(
        config_settings,
        "POLYMARKET_WC2026_SCOPE_KEYSET_RELATED_TAGS",
        False,
        raising=False,
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED", False, raising=False
    )
    monkeypatch.setattr(
        config_settings,
        "POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN",
        10000.0,
        raising=False,
    )
    monkeypatch.setattr(
        scope_predicates_mod,
        "_settings",
        config_settings,
        raising=False,
    )
    monkeypatch.setattr(scope_scan_mod, "_settings", config_settings, raising=False)
