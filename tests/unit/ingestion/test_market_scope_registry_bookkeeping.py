"""Unit tests for WC2026 registry bookkeeping."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from tests.unit.ingestion.market_scope_test_support import slug_only_cfg

from oddsfox_pipeline.ingestion.polymarket import market_scope as scope_mod
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    MarketScopeConfig,
    MarketScopeEventsScanResult,
    refresh_registry_from_events,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    registry as scope_registry_mod,
)


def test_market_scope_discovery_ledger_and_scope_hash(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        MarketScopeConfig,
        scope_config_hash,
    )
    from oddsfox_pipeline.storage.duckdb.metadata import (
        get_market_scope_discovery_fully_checked,
        get_market_scope_discovery_scope_config_hash,
        set_market_scope_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "ledger.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = MarketScopeConfig(
        event_slugs=("slug-a",),
        event_slug_prefixes=("prefix",),
        market_ids=("m1",),
        registry_max_event_pages=None,
    )
    digest = scope_config_hash(cfg)
    assert digest
    set_market_scope_discovery_fully_checked(
        fully_checked=True, scope_config_hash=digest
    )
    assert get_market_scope_discovery_fully_checked() is True
    assert get_market_scope_discovery_scope_config_hash() == digest


def test_discovery_ledger_invalidates_on_scope_change(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.storage.duckdb.metadata import (
        get_market_scope_discovery_fully_checked,
        get_market_scope_discovery_scope_config_hash,
        set_market_scope_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "hash_change.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg_a = MarketScopeConfig(
        event_slugs=("slug-a",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )
    set_market_scope_discovery_fully_checked(
        fully_checked=True, scope_config_hash=scope_mod.scope_config_hash(cfg_a)
    )
    cfg_b = MarketScopeConfig(
        event_slugs=("slug-b",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )
    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "x", "slug": "other", "markets": []}],
        "next_cursor": "more",
    }
    refresh_registry_from_events(client, config=cfg_b, max_pages=1)
    assert get_market_scope_discovery_fully_checked() is False
    assert (
        get_market_scope_discovery_scope_config_hash()
        == scope_mod.scope_config_hash(cfg_b)
    )


def test_get_registry_event_slugs(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
        RegistryRow,
        get_registry_event_slugs,
        upsert_registry_rows,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "slugs.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    upsert_registry_rows(
        [
            RegistryRow("m1", "slug-b", "ev", "seed"),
            RegistryRow("m2", "slug-a", "ev", "seed"),
            RegistryRow("m3", None, None, "seed"),
        ]
    )
    assert get_registry_event_slugs() == ["slug-a", "slug-b"]


def test_registry_seed_rows_and_dedupe_priority() -> None:
    from oddsfox_pipeline.storage.duckdb.market_scope_registry import RegistryRow

    cfg = MarketScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=("seed-m",),
        registry_max_event_pages=1,
    )

    assert any(
        r.market_id == "seed-m" for r in scope_registry_mod._seed_registry_rows(cfg)
    )
    merged = scope_registry_mod._dedupe_registry_rows(
        [
            RegistryRow("x", None, None, "seed"),
            RegistryRow("x", "es", "e1", "events_api"),
        ]
    )
    assert merged[0].source == "events_api"
    merged_same = scope_registry_mod._dedupe_registry_rows(
        [
            RegistryRow("y", None, None, "events_api"),
            RegistryRow("y", "a", None, "events_api"),
        ]
    )
    assert len(merged_same) == 1
    assert scope_registry_mod._source_priority("unknown") == 0


def test_finalize_registry_collect_meta_branches(monkeypatch) -> None:

    monkeypatch.setattr(
        scope_registry_mod,
        "upsert_registry_rows",
        lambda rows: len(rows),
    )
    cfg = slug_only_cfg()
    scan = MarketScopeEventsScanResult(
        registry_rows=(),
        raw_markets=(),
        pages_done=0,
        truncated=False,
        discovered_slugs=(),
        crawl_tag_slugs=("crawl-a",),
        scope_tag_slugs=("fifa-world-cup",),
        tag_sources=(("crawl-a", ("seed",)),),
    )
    reg, _markets, meta = scope_registry_mod._finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=scope_registry_mod.DISCOVERY_MODE_TARGETED,
        t0=time.monotonic(),
        keyset_closed=True,
        keyset_tag_slugs=["fallback-tag"],
        keyset_volume_min=100.0,
    )
    assert reg["keyset_closed"] is True
    assert reg["crawl_tag_slugs"] == ["crawl-a"]
    assert meta["keyset_volume_min"] == 100.0

    reg2, _markets2, meta2 = scope_registry_mod._finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=scope_registry_mod.DISCOVERY_MODE_TARGETED,
        t0=time.monotonic(),
        keyset_closed=None,
        keyset_volume_min=None,
    )
    assert "keyset_closed" not in reg2
    assert "keyset_closed" not in meta2
