from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

from dagster import AssetKey, AssetSpec

from oddsfox_pipeline.orchestration import (
    assets_polymarket_us_midterms_2026 as assets_mod,
)
from oddsfox_pipeline.orchestration import config as orch_config
from oddsfox_pipeline.orchestration.assets import (
    polymarket_us_midterms_2026_ops_market_scope_registry,
    polymarket_us_midterms_2026_raw_market_metadata_backfill,
    polymarket_us_midterms_2026_raw_markets,
    polymarket_us_midterms_2026_raw_markets_snapshot,
    polymarket_us_midterms_2026_raw_token_odds_history_hourly,
)


def test_midterms_dlt_translator_rewrites_markets_asset_key(monkeypatch):
    base_spec = AssetSpec(key=AssetKey(["placeholder"]))
    monkeypatch.setattr(
        assets_mod.DagsterDltTranslator,
        "get_asset_spec",
        lambda _self, _data: base_spec,
    )
    translator = assets_mod.PolymarketUsMidterms2026DltTranslator()
    data = SimpleNamespace(
        resource=SimpleNamespace(
            source_name="polymarket_us_midterms_2026",
            name="markets",
        )
    )
    spec = translator.get_asset_spec(data)
    assert spec.key == assets_mod.POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS


def test_snapshot_refreshed_scope_name_handles_missing_and_present():
    assert assets_mod._snapshot_refreshed_scope_name({}) is None
    assert assets_mod._snapshot_refreshed_scope_name(
        {"scope_name": "us_midterms_2026"}
    ) == ("us_midterms_2026")


def test_midterms_dlt_asset_clears_pending_packages_and_indexes(monkeypatch):
    pipeline = MagicMock(has_pending_data=True)
    conn = MagicMock()
    source = object()
    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter(["event"])

    @contextmanager
    def connection():
        yield conn

    def collect_payload(**kwargs):
        progress = kwargs["progress_callback"]
        progress("events_page", {"events_pages": 2})
        progress("markets_fetch", {"markets_fetched": 3})
        return {
            "market_rows": [{"id": "raw"}],
            "token_rows": [("raw", '["tok-yes", "tok-no"]')],
            "run_summary": {"task": "sync_markets", "total_fetched": 1},
        }

    monkeypatch.setattr(
        assets_mod.asset_helpers,
        "get_polymarket_dlt_pipeline",
        lambda **_kwargs: pipeline,
    )
    saved_tokens = []
    saved_metrics = {}
    monkeypatch.setattr(assets_mod, "collect_market_scope_payload", collect_payload)
    monkeypatch.setattr(
        assets_mod,
        "save_market_tokens_batch",
        lambda rows, scope_name=None: saved_tokens.extend(rows),
    )
    monkeypatch.setattr(
        assets_mod,
        "save_sync_run_metrics",
        lambda task, metrics, **kwargs: saved_metrics.update({"task": task, **metrics}),
    )
    monkeypatch.setattr(
        assets_mod,
        "polymarket_us_midterms_2026_markets_source",
        lambda *, rows=(): source,
    )
    monkeypatch.setattr(assets_mod, "get_connection", connection)
    ensure_indexes = MagicMock()
    monkeypatch.setattr(assets_mod, "ensure_polymarket_indexes", ensure_indexes)

    fn = polymarket_us_midterms_2026_raw_markets.op.compute_fn.decorated_fn
    assert list(fn(MagicMock(), orch_config.MarketsSyncConfig(), fake_dlt)) == ["event"]

    pipeline.drop_pending_packages.assert_called_once()
    fake_dlt.run.assert_called_once()
    assert saved_tokens == [("raw", '["tok-yes", "tok-no"]')]
    assert saved_metrics["task"] == "sync_markets"
    ensure_indexes.assert_called_once_with(conn, scope_name="us_midterms_2026")


def test_midterms_raw_markets_snapshot_is_local_only(monkeypatch):
    sync_markets = MagicMock()
    monkeypatch.setattr(assets_mod.ops, "sync_markets", sync_markets)
    monkeypatch.setattr(assets_mod, "format_raw_snapshot_log", lambda _snapshot: "")
    snapshots = iter([{"pre": 1}, {"post": 1}])
    monkeypatch.setattr(
        assets_mod, "snapshot_raw_layer", lambda **_kwargs: next(snapshots)
    )
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = polymarket_us_midterms_2026_raw_markets_snapshot.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.MarketsSyncConfig())

    sync_markets.assert_not_called()
    assert set(result.metadata) == {
        "source",
        "duckdb_raw_pre",
        "duckdb_raw_post",
        "duckdb_raw_delta",
        "run_summary",
    }


def test_midterms_market_scope_registry_skips_when_snapshot_already_refreshed(
    monkeypatch,
):
    monkeypatch.setattr(
        assets_mod,
        "get_sync_run_metrics",
        lambda task: {
            "registry_refreshed": True,
            "scope_name": "us_midterms_2026",
            "task": task,
        },
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {"x": 1})

    fn = (
        polymarket_us_midterms_2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    )
    result = fn(MagicMock(), orch_config.MarketScopeRegistryConfig())

    run_summary = result.metadata["run_summary"].value
    assert run_summary["skipped"] is True
    assert run_summary["reason"] == "snapshot_refreshed_registry"


def test_midterms_market_scope_registry_runs_sync(monkeypatch):
    captured = {}

    def sync_market_scope_registry(**kwargs):
        kwargs["progress_callback"]("registry_probe", {"ok": True})
        captured.update(kwargs)
        return {"registry_rows_upserted": 1}

    monkeypatch.setattr(assets_mod, "get_sync_run_metrics", lambda _task: None)
    monkeypatch.setattr(
        assets_mod.ops, "sync_market_scope_registry", sync_market_scope_registry
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = (
        polymarket_us_midterms_2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    )
    ctx = MagicMock()
    result = fn(ctx, orch_config.MarketScopeRegistryConfig())

    assert captured["scope_name"] == "us_midterms_2026"
    assert result.metadata["run_summary"].value == {"registry_rows_upserted": 1}
    assert any("registry_probe" in str(c) for c in ctx.log.info.call_args_list)


def test_midterms_market_scope_registry_force_refresh_bypasses_snapshot_metric_check(
    monkeypatch,
):
    checked_metrics = []
    monkeypatch.setattr(
        assets_mod,
        "get_sync_run_metrics",
        lambda task: checked_metrics.append(task) or {"registry_refreshed": True},
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_market_scope_registry",
        lambda **_kwargs: {"registry_rows_upserted": 1},
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = (
        polymarket_us_midterms_2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    )
    result = fn(
        MagicMock(),
        orch_config.MarketScopeRegistryConfig(force_refresh=True),
    )

    assert checked_metrics == []
    assert result.metadata["run_summary"].value == {"registry_rows_upserted": 1}


def test_midterms_dlt_asset_skips_pending_package_drop_when_clean(monkeypatch):
    pipeline = MagicMock(has_pending_data=False)
    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter([])

    monkeypatch.setattr(
        assets_mod.asset_helpers,
        "get_polymarket_dlt_pipeline",
        lambda **_kwargs: pipeline,
    )
    monkeypatch.setattr(
        assets_mod,
        "collect_market_scope_payload",
        lambda **_kwargs: {
            "market_rows": [],
            "token_rows": [],
            "run_summary": {"task": "sync_markets"},
        },
    )
    monkeypatch.setattr(
        assets_mod, "save_market_tokens_batch", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        assets_mod, "save_sync_run_metrics", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        assets_mod,
        "polymarket_us_midterms_2026_markets_source",
        lambda *, rows=(): object(),
    )

    @contextmanager
    def connection():
        yield MagicMock()

    monkeypatch.setattr(assets_mod, "get_connection", connection)
    monkeypatch.setattr(
        assets_mod, "ensure_polymarket_indexes", lambda *_args, **_kwargs: None
    )

    fn = polymarket_us_midterms_2026_raw_markets.op.compute_fn.decorated_fn
    list(fn(MagicMock(), orch_config.MarketsSyncConfig(), fake_dlt))

    pipeline.drop_pending_packages.assert_not_called()


def test_midterms_metadata_backfill_no_orphan_cleanup_log(monkeypatch):
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})
    monkeypatch.setattr(
        assets_mod.ops,
        "backfill_market_metadata",
        lambda **_kwargs: {"task": "backfill_market_metadata", "saved": {}},
    )
    monkeypatch.setattr(
        assets_mod.ops, "delete_orphan_market_tokens", lambda **_kwargs: 0
    )

    ctx = MagicMock()
    fn = polymarket_us_midterms_2026_raw_market_metadata_backfill.op.compute_fn.decorated_fn
    result = fn(ctx, orch_config.MetadataBackfillConfig())

    assert result.metadata["orphan_market_tokens_removed"].value == 0
    joined = " ".join(str(c) for c in ctx.log.info.call_args_list)
    assert "orphan market_tokens" not in joined


def test_midterms_metadata_backfill_progress_and_orphans(monkeypatch):
    captured = {}

    def backfill_market_metadata(**kwargs):
        kwargs["progress_callback"]("batch", {"batch": 1})
        captured.update(kwargs)
        return {"task": "backfill_market_metadata", "saved": {}}

    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})
    monkeypatch.setattr(
        assets_mod.ops, "backfill_market_metadata", backfill_market_metadata
    )
    monkeypatch.setattr(
        assets_mod.ops, "delete_orphan_market_tokens", lambda **_kwargs: 2
    )

    ctx = MagicMock()
    fn = polymarket_us_midterms_2026_raw_market_metadata_backfill.op.compute_fn.decorated_fn
    result = fn(ctx, orch_config.MetadataBackfillConfig())

    assert captured["market_scope"] == "us_midterms_2026"
    assert result.metadata["backfill_summaries"].value[0]["task"] == (
        "backfill_market_metadata"
    )
    assert result.metadata["orphan_market_tokens_removed"].value == 2
    joined = " ".join(str(c) for c in ctx.log.info.call_args_list)
    assert "orphan market_tokens" in joined


def test_midterms_odds_asset_delegates_to_materializer(monkeypatch):
    calls = []

    def materialize(_context, config, **kwargs):
        calls.append((type(config), kwargs))
        return "ok"

    monkeypatch.setattr(assets_mod.asset_helpers, "_materialize_odds_sync", materialize)

    ctx = MagicMock()
    assert (
        polymarket_us_midterms_2026_raw_token_odds_history_hourly.op.compute_fn.decorated_fn(
            ctx, orch_config.HourlyOddsSyncConfig()
        )
        == "ok"
    )
    assert calls[0][0] is orch_config.HourlyOddsSyncConfig
    assert calls[0][1]["market_scope"] == "us_midterms_2026"


def test_midterms_odds_asset_run_with_raw_snapshot_wrapper(monkeypatch):
    captured = {}

    def sync_odds(**kwargs):
        captured.update(kwargs)
        return {
            "planning": {"plans": 1},
            "planning_context": {"markets": 1},
            "totals": {"rows": 2},
        }

    monkeypatch.setattr(assets_mod.ops, "sync_odds", sync_odds)
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = polymarket_us_midterms_2026_raw_token_odds_history_hourly.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.HourlyOddsSyncConfig())

    assert captured["market_scope"] == "us_midterms_2026"
    assert result.metadata["totals"].value == {"rows": 2}
