from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

from dagster import AssetKey, AssetSpec

from oddsfox_pipeline.orchestration import assets_kalshi_wc2026 as assets_mod
from oddsfox_pipeline.orchestration import config as orch_config
from oddsfox_pipeline.orchestration.assets import (
    kalshi_wc2026_ops_market_scope_registry,
    kalshi_wc2026_raw_market_candlesticks_hourly,
    kalshi_wc2026_raw_markets,
    kalshi_wc2026_raw_markets_snapshot,
)


def test_get_kalshi_dlt_pipeline_uses_path_cache(monkeypatch):
    created = []

    class FakeDlt:
        class destinations:
            @staticmethod
            def duckdb(*, credentials):
                return {"credentials": credentials}

        @staticmethod
        def pipeline(**kwargs):
            created.append(kwargs)
            return object()

    assets_mod.asset_helpers._DLT_PIPELINE_BY_PATH.clear()
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)

    first = assets_mod.asset_helpers.get_kalshi_dlt_pipeline(
        active_duckdb_path_fn=lambda: "/tmp/cache.duckdb",
        dlt_module=FakeDlt,
    )
    second = assets_mod.asset_helpers.get_kalshi_dlt_pipeline(
        active_duckdb_path_fn=lambda: "/tmp/cache.duckdb",
        dlt_module=FakeDlt,
    )

    assert first is second
    assert len(created) == 1
    assert created[0]["pipeline_name"] == "kalshi_wc2026_raw_landing"

    assets_mod.asset_helpers._DLT_PIPELINE_BY_PATH.clear()
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw2")
    assets_mod.asset_helpers.get_kalshi_dlt_pipeline(
        active_duckdb_path_fn=lambda: "/tmp/cache.duckdb",
        dlt_module=FakeDlt,
    )
    assert created[-1]["pipeline_name"] == "kalshi_wc2026_raw_gw2_landing"
    assets_mod.asset_helpers._DLT_PIPELINE_BY_PATH.clear()


def test_kalshi_dlt_translator_rewrites_markets_asset_key(monkeypatch):
    base_spec = AssetSpec(key=AssetKey(["placeholder"]))
    monkeypatch.setattr(
        assets_mod.DagsterDltTranslator,
        "get_asset_spec",
        lambda _self, _data: base_spec,
    )
    translator = assets_mod.KalshiWc2026DltTranslator()
    data = SimpleNamespace(
        resource=SimpleNamespace(
            source_name="kalshi_wc2026",
            name="events",
        )
    )
    events_spec = translator.get_asset_spec(data)
    assert events_spec.key == assets_mod.KALSHI_WC2026_RAW_EVENTS

    data.resource.name = "markets"
    markets_spec = translator.get_asset_spec(data)
    assert markets_spec.key == assets_mod.KALSHI_WC2026_RAW_MARKETS


def test_snapshot_refreshed_scope_name_handles_missing_and_present():
    assert assets_mod._snapshot_refreshed_scope_name({}) is None
    assert assets_mod._snapshot_refreshed_scope_name({"scope_name": "wc2026"}) == (
        "wc2026"
    )


def test_dlt_asset_clears_pending_packages_and_indexes(monkeypatch):
    pipeline = MagicMock(has_pending_data=True)
    conn = MagicMock()
    source = object()
    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter(["event"])

    @contextmanager
    def connection():
        yield conn

    monkeypatch.setattr(
        assets_mod.asset_helpers,
        "get_kalshi_dlt_pipeline",
        lambda **_kwargs: pipeline,
    )
    saved_metrics = {}
    monkeypatch.setattr(
        assets_mod,
        "collect_market_scope_payload",
        lambda **_kwargs: {
            "scope_name": "wc2026",
            "events": [{"event_ticker": "KXWC-EVT1"}],
            "markets": [{"market_ticker": "KXWC-MKT1"}],
            "total_events": 1,
            "total_markets": 1,
            "registry_summary": {"registry_rows_upserted": 1},
        },
    )
    monkeypatch.setattr(
        assets_mod,
        "save_sync_run_metrics",
        lambda task, metrics, **kwargs: saved_metrics.update(
            {"task": task, **metrics, **kwargs}
        ),
    )
    monkeypatch.setattr(
        assets_mod, "kalshi_wc2026_source", lambda *, events=(), markets=(): source
    )
    monkeypatch.setattr(assets_mod, "get_connection", connection)
    ensure_indexes = MagicMock()
    monkeypatch.setattr(assets_mod, "ensure_kalshi_indexes", ensure_indexes)

    fn = kalshi_wc2026_raw_markets.op.compute_fn.decorated_fn
    assert list(fn(MagicMock(), orch_config.KalshiMarketsSyncConfig(), fake_dlt)) == [
        "event"
    ]

    pipeline.drop_pending_packages.assert_called_once()
    fake_dlt.run.assert_called_once()
    assert saved_metrics["task"] == "sync_kalshi_markets"
    assert saved_metrics["source"] == "kalshi"
    ensure_indexes.assert_called_once_with(conn, scope_name="wc2026")


def test_raw_markets_snapshot_is_local_only(monkeypatch):
    sync_markets = MagicMock()
    monkeypatch.setattr(assets_mod.ops, "sync_kalshi_markets", sync_markets)
    monkeypatch.setattr(assets_mod, "format_raw_snapshot_log", lambda _snapshot: "")
    snapshots = iter([{"pre": 1}, {"post": 1}])
    monkeypatch.setattr(
        assets_mod, "snapshot_raw_layer", lambda **_kwargs: next(snapshots)
    )
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = kalshi_wc2026_raw_markets_snapshot.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.KalshiMarketsSyncConfig())

    sync_markets.assert_not_called()
    assert set(result.metadata) == {
        "source",
        "duckdb_raw_pre",
        "duckdb_raw_post",
        "duckdb_raw_delta",
        "run_summary",
    }


def test_market_scope_registry_skips_when_snapshot_already_refreshed(monkeypatch):
    monkeypatch.setattr(
        assets_mod,
        "get_sync_run_metrics",
        lambda task, **kwargs: {
            "registry_refreshed": True,
            "scope_name": "wc2026",
            "task": task,
            **kwargs,
        },
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {"x": 1})

    fn = kalshi_wc2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.KalshiMarketScopeRegistryConfig())

    run_summary = result.metadata["run_summary"].value
    assert run_summary["skipped"] is True
    assert run_summary["reason"] == "snapshot_refreshed_registry"


def test_market_scope_registry_runs_sync_when_snapshot_did_not_refresh(monkeypatch):
    captured = {}

    def sync_kalshi_market_scope_registry(**kwargs):
        captured.update(kwargs)
        return {"registry_rows_upserted": 1}

    monkeypatch.setattr(
        assets_mod, "get_sync_run_metrics", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_kalshi_market_scope_registry",
        sync_kalshi_market_scope_registry,
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = kalshi_wc2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    result = fn(
        MagicMock(),
        orch_config.KalshiMarketScopeRegistryConfig(force_refresh=True),
    )

    assert captured["config"].scope_name == "wc2026"
    assert result.metadata["run_summary"].value == {
        "scope_name": "wc2026",
        "registry_rows_upserted": 1,
    }


def test_market_scope_registry_force_refresh_bypasses_snapshot_metric_check(
    monkeypatch,
):
    checked_metrics = []
    monkeypatch.setattr(
        assets_mod,
        "get_sync_run_metrics",
        lambda *_args, **_kwargs: (
            checked_metrics.append("metrics") or {"registry_refreshed": True}
        ),
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_kalshi_market_scope_registry",
        lambda **_kwargs: {"registry_rows_upserted": 1},
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = kalshi_wc2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    result = fn(
        MagicMock(),
        orch_config.KalshiMarketScopeRegistryConfig(force_refresh=True),
    )

    assert checked_metrics == []
    assert result.metadata["run_summary"].value == {
        "scope_name": "wc2026",
        "registry_rows_upserted": 1,
    }


def test_candlesticks_asset_delegates_to_materializer(monkeypatch):
    calls = []

    def materialize(_context, config, **kwargs):
        calls.append((type(config), kwargs))
        return "ok"

    monkeypatch.setattr(
        assets_mod.asset_helpers, "materialize_kalshi_candlesticks_sync", materialize
    )

    ctx = MagicMock()
    assert (
        kalshi_wc2026_raw_market_candlesticks_hourly.op.compute_fn.decorated_fn(
            ctx, orch_config.KalshiHourlyOddsSyncConfig()
        )
        == "ok"
    )
    assert calls[0][0] is orch_config.KalshiHourlyOddsSyncConfig
    assert calls[0][1]["scope_name"] == "wc2026"


def test_raw_snapshot_metadata_without_run_summary():
    metadata = assets_mod.asset_helpers._raw_snapshot_metadata(
        {"pre": 1},
        {"post": 2},
        {"delta": 1},
    )
    assert "run_summary" not in metadata
    assert metadata["duckdb_raw_pre"].value == {"pre": 1}


def test_materialize_kalshi_candlesticks_sync_builds_metadata(monkeypatch):
    from oddsfox_pipeline.orchestration import kalshi_asset_helpers as helpers

    def sync_fn(**kwargs):
        if kwargs.get("progress_callback"):
            kwargs["progress_callback"](
                "kalshi_candlesticks",
                {"markets_synced": 2, "rows_written": 5},
            )
        return {"markets_synced": 2, "rows_written": 5}

    def run_with_snapshot(_level, run_fn):
        summary = run_fn({})
        return summary, {}, {}, {}, helpers._raw_snapshot_metadata({}, {}, {})

    result = helpers.materialize_kalshi_candlesticks_sync(
        MagicMock(),
        orch_config.KalshiHourlyOddsSyncConfig(window_hours=12, force=True),
        scope_name="wc2026",
        sync_fn=sync_fn,
        run_with_raw_snapshot_fn=run_with_snapshot,
    )

    assert result.metadata["window_hours"].value == 12
    assert result.metadata["force"].value is True
    assert result.metadata["markets_synced"].value == 2
    assert result.metadata["rows_written"].value == 5


def test_dlt_translator_returns_base_spec_for_unknown_resource(monkeypatch):
    base_spec = AssetSpec(key=AssetKey(["placeholder"]))
    monkeypatch.setattr(
        assets_mod.DagsterDltTranslator,
        "get_asset_spec",
        lambda _self, _data: base_spec,
    )
    translator = assets_mod.KalshiWc2026DltTranslator()
    data = SimpleNamespace(
        resource=SimpleNamespace(source_name="kalshi_wc2026", name="other")
    )
    assert translator.get_asset_spec(data) is base_spec


def test_dlt_asset_skips_pending_package_drop_when_clean(monkeypatch):
    pipeline = MagicMock(has_pending_data=False)
    conn = MagicMock()
    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter([])

    @contextmanager
    def connection():
        yield conn

    monkeypatch.setattr(
        assets_mod.asset_helpers,
        "get_kalshi_dlt_pipeline",
        lambda **_kwargs: pipeline,
    )

    def collect(**kwargs):
        if kwargs.get("progress_callback"):
            kwargs["progress_callback"]("kalshi_page", {"pages": 1})
        return {
            "scope_name": "wc2026",
            "events": [],
            "markets": [],
            "total_events": 0,
            "total_markets": 0,
            "registry_summary": {},
        }

    monkeypatch.setattr(assets_mod, "collect_market_scope_payload", collect)
    monkeypatch.setattr(assets_mod, "save_sync_run_metrics", lambda *_a, **_k: None)
    monkeypatch.setattr(
        assets_mod, "kalshi_wc2026_source", lambda *, events=(), markets=(): object()
    )
    monkeypatch.setattr(assets_mod, "get_connection", connection)
    monkeypatch.setattr(assets_mod, "ensure_kalshi_indexes", MagicMock())

    fn = kalshi_wc2026_raw_markets.op.compute_fn.decorated_fn
    list(fn(MagicMock(), orch_config.KalshiMarketsSyncConfig(), fake_dlt))

    pipeline.drop_pending_packages.assert_not_called()


def test_market_scope_registry_runs_when_snapshot_scope_mismatches(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        assets_mod,
        "get_sync_run_metrics",
        lambda *_args, **_kwargs: {
            "registry_refreshed": True,
            "scope_name": "other_scope",
        },
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_kalshi_market_scope_registry",
        lambda **_kwargs: (
            captured.setdefault("ran", True),
            {"registry_rows_upserted": 1},
        )[1],
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = kalshi_wc2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    fn(MagicMock(), orch_config.KalshiMarketScopeRegistryConfig())

    assert captured.get("ran") is True
