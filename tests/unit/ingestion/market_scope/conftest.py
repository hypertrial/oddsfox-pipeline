"""Deterministic discovery settings for market-scope unit tests."""

from __future__ import annotations

import pytest

from oddsfox_pipeline.config import settings as config_settings
from oddsfox_pipeline.config.settings_polymarket import (
    POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    predicates as scope_predicates_mod,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    scan as scope_scan_mod,
)


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
        POLYMARKET_WC2026_KNOCKOUT_MIN_VOLUME_USD,
        raising=False,
    )
    monkeypatch.setattr(
        scope_predicates_mod,
        "_settings",
        config_settings,
        raising=False,
    )
    monkeypatch.setattr(scope_scan_mod, "_settings", config_settings, raising=False)
