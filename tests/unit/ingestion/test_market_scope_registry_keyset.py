"""Unit tests for WC2026 keyset and tag discovery."""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.unit.ingestion.market_scope_test_support import slug_only_cfg

from oddsfox_pipeline.ingestion.polymarket import market_scope as scope_mod
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    MarketScopeEventsScanResult,
    collect_scope_markets_from_events,
    load_market_scope_config,
    refresh_registry_from_events,
)


def test_refresh_registry_from_events(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "registry.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m100"}, {"id": "m101"}],
            },
            {
                "id": "ev2",
                "slug": "premier-league-2026",
                "markets": [{"id": "m999"}],
            },
        ],
        "next_cursor": None,
    }
    summary = refresh_registry_from_events(client, config=slug_only_cfg(), max_pages=5)
    assert summary["registry_rows_upserted"] == 2
    assert "2026-fifa-world-cup-winner-595" in summary["discovered_event_slugs"]
    assert summary["by_source"] == {"events_api": 2}

    from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
        get_registry_market_ids,
    )

    assert sorted(get_registry_market_ids()) == ["m100", "m101"]


def test_markets_sync_full_keyset_mode(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.markets.sync import sync_markets

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "full_keyset.duckdb"))
    monkeypatch.setenv("POLYMARKET_WC2026_SCOPE_TAG_DISCOVERY", "false")
    monkeypatch.delenv("POLYMARKET_WC2026_SCOPE_EVENT_TAGS", raising=False)
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [
                    {
                        "id": "m1",
                        "question": "Q",
                        "outcomes": "[]",
                        "clobTokenIds": '["t1"]',
                    },
                ],
            },
        ],
        "next_cursor": None,
    }
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.sync.build_client",
        lambda: client,
    )

    cfg = load_market_scope_config()
    result = sync_markets(discovery_mode="full_keyset")
    assert result["discovery_mode"] == "full_keyset"
    keyset_calls = [
        c
        for c in client.get.call_args_list
        if c.args and str(c.args[0]).endswith("/events/keyset")
    ]
    assert len(keyset_calls) == len(cfg.event_tags)
    assert {c.kwargs["params"]["tag_slug"] for c in keyset_calls} == set(cfg.event_tags)
    assert result["total_fetched"] >= 1


def test_refresh_registry_from_events_keyset_closed_filter(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        refresh_registry_from_events,
    )

    page1 = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
        ],
        "next_cursor": None,
    }
    client = MagicMock()
    client.get.return_value = page1

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "keyset_closed.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg()
    refresh_registry_from_events(client, config=cfg, max_pages=10, keyset_closed=False)
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("closed") is False


def test_refresh_registry_from_events_keyset_tag_and_volume_filters(
    monkeypatch, tmp_path
):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        refresh_registry_from_events,
    )

    page1 = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
        ],
        "next_cursor": None,
    }
    client = MagicMock()
    client.get.return_value = page1

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "keyset_filters.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg()
    summary = refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=10,
        keyset_closed=False,
        keyset_tag_slugs=["fifa-world-cup", "2026-fifa-world-cup"],
        keyset_volume_min=5000,
    )
    assert client.get.call_count == 2
    tag_slugs = [
        (c.kwargs.get("params") or {}).get("tag_slug")
        for c in client.get.call_args_list
    ]
    assert tag_slugs == ["fifa-world-cup", "2026-fifa-world-cup"]
    for call in client.get.call_args_list:
        params = call.kwargs.get("params") or {}
        assert params.get("closed") is False
        assert params.get("volume_min") == 5000
    assert summary["keyset_tag_slugs"] == ["fifa-world-cup", "2026-fifa-world-cup"]
    assert summary["keyset_volume_min"] == 5000


def test_full_keyset_stops_after_pages_without_progress(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        collect_scope_markets_from_events,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "no_progress.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)

    cfg = slug_only_cfg(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=(),
    )
    # Advancing cursor each page so the stall guard does not trip; this exercises
    # the distinct ``max_pages_without_progress`` (no in-scope match) stop path.
    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": "x", "slug": "other-event", "markets": [{"id": "m-x"}]}],
            "next_cursor": f"more-{i}",
        }
        for i in range(30)
    ]

    markets, meta = collect_scope_markets_from_events(
        client,
        config=cfg,
        max_pages=100,
    )
    assert markets == []
    assert meta["truncated"] is True
    assert meta["events_pages"] == 25


def test_full_keyset_marks_discovery_complete(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.storage.duckdb.metadata import (
        get_market_scope_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "complete.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=(),
    )
    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
        ],
        "next_cursor": None,
    }
    refresh_registry_from_events(client, config=cfg, max_pages=5)
    assert get_market_scope_discovery_fully_checked() is True


def test_truncated_full_keyset_clears_fully_checked(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.storage.duckdb.metadata import (
        get_market_scope_discovery_fully_checked,
        set_market_scope_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "truncated.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=(),
    )
    set_market_scope_discovery_fully_checked(
        fully_checked=True, scope_config_hash=scope_mod.scope_config_hash(cfg)
    )
    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": f"x{i}", "slug": "other", "markets": []}],
            "next_cursor": f"cursor-{i + 1}",
        }
        for i in range(30)
    ]
    refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=100,
        max_pages_without_progress=2,
    )
    assert get_market_scope_discovery_fully_checked() is False


def test_collect_markets_omits_closed_when_unset(monkeypatch):
    cfg = slug_only_cfg(keyset_closed=None)
    scan_result = MarketScopeEventsScanResult(
        registry_rows=(),
        raw_markets=(),
        pages_done=0,
        truncated=False,
        discovered_slugs=(),
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope.registry._scan_market_scope_gamma_events",
        lambda *a, **k: scan_result,
    )
    _markets, meta = collect_scope_markets_from_events(MagicMock(), config=cfg)
    assert "keyset_closed" not in meta


def test_collect_markets_meta_uses_keyset_slugs_when_no_crawl_tags(monkeypatch):
    cfg = slug_only_cfg()
    scan_result = MarketScopeEventsScanResult(
        registry_rows=(),
        raw_markets=({"id": "m1"},),
        pages_done=1,
        truncated=False,
        discovered_slugs=(),
        crawl_tag_slugs=(),
        scope_tag_slugs=("fifa-world-cup",),
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope.registry._scan_market_scope_gamma_events",
        lambda *a, **k: scan_result,
    )
    markets, meta = collect_scope_markets_from_events(
        MagicMock(),
        config=cfg,
        keyset_tag_slugs=["explicit-tag"],
        keyset_volume_min=5000.0,
    )
    assert markets[0]["id"] == "m1"
    assert meta["keyset_tag_slugs"] == ["explicit-tag"]
    assert meta["keyset_volume_min"] == 5000.0

    _markets2, meta2 = collect_scope_markets_from_events(
        MagicMock(),
        config=slug_only_cfg(keyset_volume_min=None),
        keyset_tag_slugs=["explicit-tag"],
    )
    assert "keyset_volume_min" not in meta2
