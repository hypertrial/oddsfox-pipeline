"""Unit tests for WC2026 registry discovery."""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.unit.ingestion.market_scope_test_support import slug_only_cfg

from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    MarketScopeConfig,
    refresh_registry_and_collect_markets_from_events,
    refresh_registry_from_events,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    registry as scope_registry_mod,
)


def test_refresh_registry_and_collect_single_events_pass(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "combined.duckdb"))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
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
        ],
        "next_cursor": None,
    }
    registry_summary, markets, collect_meta = (
        refresh_registry_and_collect_markets_from_events(
            client,
            config=slug_only_cfg(),
            max_pages=5,
            tag_discovery=False,
        )
    )
    assert client.get.call_count == 1
    assert registry_summary["registry_rows_upserted"] == 2
    assert len(markets) == 2
    assert collect_meta["markets_collected"] == 2
    assert markets[0].get("events")


def test_refresh_registry_with_seed_market_ids(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "seed_registry.duckdb"))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = MarketScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("seed-only",),
        registry_max_event_pages=5,
    )
    client = MagicMock()
    client.get.return_value = {"events": [], "next_cursor": None}
    summary = refresh_registry_from_events(client, config=cfg, max_pages=1)
    assert summary["registry_rows_upserted"] >= 1
    assert summary["by_source"].get("seed", 0) >= 1


def test_targeted_registry_ignores_progress_callback_failures(monkeypatch):
    cfg = MarketScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("123",),
        registry_max_event_pages=5,
    )
    monkeypatch.setattr(
        scope_registry_mod, "fetch_gamma_event_by_slug", lambda *_a: None
    )
    monkeypatch.setattr(
        scope_registry_mod, "get_registry_market_ids", lambda _scope_name: ["456"]
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_fetch_markets_batch_resilient",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_finalize_registry_collect",
        lambda *_a, **_k: ({"registry_rows_upserted": 0}, [], {}),
    )

    summary, markets, meta = (
        scope_registry_mod.refresh_registry_and_collect_markets_targeted(
            MagicMock(),
            config=cfg,
            progress_callback=lambda phase, payload: (_ for _ in ()).throw(
                RuntimeError("callback failed")
            ),
        )
    )

    assert summary == {"registry_rows_upserted": 0}
    assert markets == []
    assert meta == {}


def test_targeted_registry_emits_progress_callbacks(monkeypatch):
    cfg = MarketScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("123",),
        registry_max_event_pages=5,
    )
    monkeypatch.setattr(
        scope_registry_mod, "fetch_gamma_event_by_slug", lambda *_a: None
    )
    monkeypatch.setattr(
        scope_registry_mod, "get_registry_market_ids", lambda _scope_name: ["456"]
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_fetch_markets_batch_resilient",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_finalize_registry_collect",
        lambda *_a, **_k: ({"registry_rows_upserted": 0}, [], {}),
    )
    progress = []

    scope_registry_mod.refresh_registry_and_collect_markets_targeted(
        MagicMock(),
        config=cfg,
        progress_callback=lambda phase, payload: progress.append((phase, payload)),
    )

    assert [phase for phase, _payload in progress] == [
        "market_scope_event_by_slug",
        "market_scope_markets_by_id",
    ]


def test_targeted_registry_without_progress_callback(monkeypatch):
    cfg = MarketScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("123",),
        registry_max_event_pages=5,
    )
    monkeypatch.setattr(
        scope_registry_mod, "fetch_gamma_event_by_slug", lambda *_a: None
    )
    monkeypatch.setattr(
        scope_registry_mod, "get_registry_market_ids", lambda _scope_name: ["456"]
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_fetch_markets_batch_resilient",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_finalize_registry_collect",
        lambda *_a, **_k: ({"registry_rows_upserted": 0}, [], {}),
    )

    summary, markets, meta = (
        scope_registry_mod.refresh_registry_and_collect_markets_targeted(
            MagicMock(),
            config=cfg,
        )
    )

    assert summary == {"registry_rows_upserted": 0}
    assert markets == []
    assert meta == {}


def test_markets_sync_targeted_discovery(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.markets.sync import sync_markets

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "targeted.duckdb"))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    event_payload = {
        "id": "ev1",
        "slug": "2026-fifa-world-cup-winner",
        "markets": [
            {
                "id": "253591",
                "question": "Q",
                "outcomes": "[]",
                "clobTokenIds": '["t1"]',
            },
        ],
    }

    def _fake_get(path, **kwargs):
        if path.endswith("/events/slug/2026-fifa-world-cup-winner"):
            return event_payload
        if path == "/markets":
            return []
        raise AssertionError(f"unexpected path: {path}")

    client = MagicMock()
    client.get.side_effect = _fake_get
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.sync.build_client",
        lambda: client,
    )
    progress = []

    result = sync_markets(
        discovery_mode="targeted",
        progress_callback=lambda phase, payload: progress.append(phase),
    )
    assert result["mode"] == "market_scope_event_first"
    assert result["discovery_mode"] == "targeted"
    assert result["total_fetched"] >= 1
    assert "market_scope_event_by_slug" in progress
    assert "discovery_complete" in progress

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.sync.prepare_batch_for_db",
        lambda df: ([], []),
    )
    progress.clear()
    result_empty_batch = sync_markets(
        discovery_mode="targeted",
        progress_callback=lambda phase, payload: progress.append(phase),
    )
    assert result_empty_batch["total_fetched"] == 0
    assert "discovery_complete" in progress

    result_no_cb = sync_markets(discovery_mode="targeted")
    assert result_no_cb["mode"] == "market_scope_event_first"

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.markets.sync.refresh_registry_and_collect_markets_targeted",
        lambda *a, **k: ({"registry_rows_upserted": 0}, [], {"markets_collected": 0}),
    )
    empty_events = sync_markets(discovery_mode="targeted")
    assert empty_events["total_fetched"] == 0


def test_refresh_registry_targeted_slug_and_markets(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        MarketScopeConfig,
        refresh_registry_and_collect_markets_targeted,
    )

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "targeted_registry.duckdb"))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = MarketScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("1001",),
        registry_max_event_pages=None,
    )
    progress = []

    def _fake_get(path, **kwargs):
        if path.endswith("/events/slug/2026-fifa-world-cup-winner-595"):
            return {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "2001"}],
            }
        if path == "/markets":
            return [
                {
                    "id": "1001",
                    "events": [{"slug": "2026-fifa-world-cup-winner-595", "id": "ev1"}],
                }
            ]
        raise AssertionError(path)

    client = MagicMock()
    client.get.side_effect = _fake_get
    summary, markets, meta = refresh_registry_and_collect_markets_targeted(
        client,
        config=cfg,
        progress_callback=lambda phase, payload: progress.append(phase),
    )
    assert summary["discovery_mode"] == "targeted"
    assert summary["registry_refreshed"] is True
    assert meta["api_requests"] >= 2
    assert len(markets) >= 2
    assert "market_scope_event_by_slug" in progress
    assert "market_scope_markets_by_id" in progress


def test_targeted_skips_missing_slug_and_markets_callback_errors(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.errors import GammaRequestError
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        MarketScopeConfig,
        refresh_registry_and_collect_markets_targeted,
    )

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "targeted_skip.duckdb"))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = MarketScopeConfig(
        event_slugs=("missing-slug", "slug-a"),
        event_slug_prefixes=(),
        market_ids=("9001",),
        registry_max_event_pages=None,
    )

    def _fake_get(path, **kwargs):
        if path.endswith("/events/slug/missing-slug"):
            response = MagicMock()
            response.status_code = 404
            raise GammaRequestError("missing", response=response)
        if path.endswith("/events/slug/slug-a"):
            return {"id": "ev", "slug": "slug-a", "markets": []}
        if path == "/markets":
            return [{"id": "9001", "events": [{"slug": "slug-a", "id": "ev"}]}]
        raise AssertionError(path)

    client = MagicMock()
    client.get.side_effect = _fake_get
    calls = {"count": 0}

    def _progress(phase, payload):
        calls["count"] += 1
        if phase == "market_scope_markets_by_id":
            raise RuntimeError("markets progress failed")

    summary, _, _ = refresh_registry_and_collect_markets_targeted(
        client,
        config=cfg,
        progress_callback=_progress,
    )
    assert summary["discovery_mode"] == "targeted"
    assert calls["count"] >= 2


def test_targeted_progress_callbacks_ignore_failures(monkeypatch, tmp_path):
    import importlib

    import oddsfox_pipeline.storage.duckdb.connection as connection
    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules
    from oddsfox_pipeline.ingestion.polymarket.market_scope import (
        MarketScopeConfig,
        refresh_registry_and_collect_markets_targeted,
    )

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "cb_fail.duckdb"))
    reload_all_settings_modules()
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = MarketScopeConfig(
        event_slugs=("slug-a",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )

    def _fake_get(path, **kwargs):
        if "/events/slug/" in path:
            return {"id": "ev", "slug": "slug-a", "markets": []}
        return []

    client = MagicMock()
    client.get.side_effect = _fake_get

    def _boom(*args, **kwargs):
        raise RuntimeError("progress failed")

    summary, _, _ = refresh_registry_and_collect_markets_targeted(
        client,
        config=cfg,
        progress_callback=_boom,
    )
    assert summary["discovery_mode"] == "targeted"
