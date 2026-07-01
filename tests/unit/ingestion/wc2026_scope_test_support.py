"""Shared helpers for WC2026 scope unit tests."""

from __future__ import annotations

import pytest

from oddsfox.config import settings as config_settings
from oddsfox.ingestion.polymarket.wc2026_scope import Wc2026ScopeConfig
from oddsfox.ingestion.polymarket.wc2026_scope import (
    predicates as scope_predicates_mod,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    scan as scope_scan_mod,
)


def slug_only_cfg(**kwargs) -> Wc2026ScopeConfig:
    defaults = {
        "event_slugs": ("2026-fifa-world-cup-winner-595",),
        "event_slug_prefixes": ("2026-fifa-world-cup",),
        "market_ids": (),
        "registry_max_event_pages": None,
        "event_tags": (),
    }
    defaults.update(kwargs)
    return Wc2026ScopeConfig(**defaults)


@pytest.fixture(autouse=True)
def _wc2026_test_discovery_settings(monkeypatch):
    """Keep unit tests deterministic (no live Gamma tag discovery)."""
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_TAG_DISCOVERY", False, raising=False
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS", 0, raising=False
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_TAG_CRAWL_MAX", 100, raising=False
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_KEYSET_RELATED_TAGS", False, raising=False
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_KEYSET_CLOSED", False, raising=False
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_KEYSET_VOLUME_MIN", 10000.0, raising=False
    )
    monkeypatch.setattr(
        scope_predicates_mod,
        "_settings",
        config_settings,
        raising=False,
    )
    monkeypatch.setattr(scope_scan_mod, "_settings", config_settings, raising=False)
