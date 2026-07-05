from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
from oddsfox_pipeline.orchestration import config as orch_config
from oddsfox_pipeline.orchestration import polymarket_asset_helpers as helpers_mod
from oddsfox_pipeline.orchestration.assets import (
    polymarket_wc2026_ops_market_scope_registry,
    polymarket_wc2026_raw_markets,
    polymarket_wc2026_raw_markets_snapshot,
    polymarket_wc2026_raw_token_odds_history_hourly,
)


def test_get_polymarket_dlt_pipeline_uses_path_cache():
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

    helpers_mod._DLT_PIPELINE_BY_PATH.clear()

    first = helpers_mod.get_polymarket_dlt_pipeline(
        active_duckdb_path_fn=lambda: "/tmp/cache.duckdb",
        dlt_module=FakeDlt,
    )
    second = helpers_mod.get_polymarket_dlt_pipeline(
        active_duckdb_path_fn=lambda: "/tmp/cache.duckdb",
        dlt_module=FakeDlt,
    )

    assert first is second
    assert len(created) == 1


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
        "get_polymarket_dlt_pipeline",
        lambda **_kwargs: pipeline,
    )
    token_rows = [("raw", '["tok-yes", "tok-no"]')]
    saved_tokens = []
    saved_metrics = {}
    monkeypatch.setattr(
        assets_mod,
        "collect_market_scope_payload",
        lambda **_kwargs: {
            "market_rows": [{"id": "raw"}],
            "token_rows": token_rows,
            "run_summary": {"task": "sync_markets", "total_fetched": 1},
        },
    )
    monkeypatch.setattr(
        assets_mod,
        "save_market_tokens_batch",
        lambda rows: saved_tokens.extend(rows),
    )
    monkeypatch.setattr(
        assets_mod,
        "save_sync_run_metrics",
        lambda task, metrics: saved_metrics.update({"task": task, **metrics}),
    )
    monkeypatch.setattr(
        assets_mod, "polymarket_markets_source", lambda *, rows=(): source
    )
    monkeypatch.setattr(assets_mod, "get_connection", connection)
    ensure_indexes = MagicMock()
    monkeypatch.setattr(assets_mod, "ensure_polymarket_indexes", ensure_indexes)

    fn = polymarket_wc2026_raw_markets.op.compute_fn.decorated_fn
    assert list(fn(MagicMock(), orch_config.MarketsSyncConfig(), fake_dlt)) == ["event"]

    pipeline.drop_pending_packages.assert_called_once()
    fake_dlt.run.assert_called_once()
    assert saved_tokens == token_rows
    assert saved_metrics["task"] == "sync_markets"
    ensure_indexes.assert_called_once_with(conn)


def test_raw_markets_snapshot_is_local_only(monkeypatch):
    sync_markets = MagicMock()
    monkeypatch.setattr(assets_mod.ops, "sync_markets", sync_markets)
    monkeypatch.setattr(assets_mod, "format_raw_snapshot_log", lambda _snapshot: "")
    snapshots = iter([{"pre": 1}, {"post": 1}])
    monkeypatch.setattr(
        assets_mod, "snapshot_raw_layer", lambda **_kwargs: next(snapshots)
    )
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = polymarket_wc2026_raw_markets_snapshot.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.MarketsSyncConfig())

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
        lambda task: {
            "registry_refreshed": True,
            "scope_name": "wc2026",
            "task": task,
        },
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {"x": 1})

    fn = polymarket_wc2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.MarketScopeRegistryConfig())

    run_summary = result.metadata["run_summary"].value
    assert run_summary["skipped"] is True
    assert run_summary["reason"] == "snapshot_refreshed_registry"


def test_market_scope_registry_runs_sync_when_snapshot_did_not_refresh(monkeypatch):
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

    fn = polymarket_wc2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    result = fn(
        MagicMock(),
        orch_config.MarketScopeRegistryConfig(
            max_event_pages=3,
            max_pages_without_progress=2,
            keyset_closed=None,
            keyset_tag_slugs=["world-cup"],
            keyset_volume_min=None,
        ),
    )

    assert captured["max_event_pages"] == 3
    assert captured["max_pages_without_progress"] == 2
    assert captured["keyset_closed"] is None
    assert captured["keyset_tag_slugs"] == ["world-cup"]
    assert captured["keyset_volume_min"] is None
    assert result.metadata["run_summary"].value == {
        "registry_rows_upserted": 1,
    }


def test_market_scope_registry_force_refresh_bypasses_snapshot_metric_check(
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

    fn = polymarket_wc2026_ops_market_scope_registry.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.MarketScopeRegistryConfig(force_refresh=True))

    assert checked_metrics == []
    assert result.metadata["run_summary"].value == {
        "registry_rows_upserted": 1,
    }


def test_materialize_odds_sync_metadata_and_plan_iterator():
    captured = {}

    def sync_odds(**kwargs):
        captured.update(kwargs)
        return {
            "planning": {"plans": 1},
            "planning_context": {"markets": 1},
            "totals": {"rows": 2},
        }

    def run_with_snapshot(_level, run_fn):
        return run_fn({}), {}, {}, {}, {}

    plan_iterator = object()
    result = helpers_mod._materialize_odds_sync(
        MagicMock(),
        orch_config.OddsSyncConfig(min_volume=10.0, max_volume=20.0),
        plan_iterator_factory=plan_iterator,
        sync_odds_fn=sync_odds,
        run_with_raw_snapshot_fn=run_with_snapshot,
    )

    assert captured["plan_iterator_factory"] is plan_iterator
    assert result.metadata["min_volume"].value == 10.0
    assert result.metadata["max_volume"].value == 20.0


def test_odds_sync_helper_builds_kwargs_and_metadata():
    progress = MagicMock()
    plan_iterator = object()
    config = orch_config.OddsSyncConfig(
        workers=3,
        min_volume=10.0,
        max_volume=20.0,
    )

    kwargs = helpers_mod._build_odds_sync_kwargs(
        config,
        progress,
        plan_iterator_factory=plan_iterator,
    )
    metadata = helpers_mod._odds_sync_metadata(
        config,
        {
            "planning": {"plans": 1},
            "planning_context": {"markets": 2},
            "totals": {"rows": 3},
        },
        {},
    )

    assert kwargs["max_workers"] == 3
    assert kwargs["progress_callback"] is progress
    assert kwargs["plan_iterator_factory"] is plan_iterator
    assert kwargs["market_scope"] == "wc2026"
    assert metadata["workers"].value == 3
    assert metadata["min_volume"].value == 10.0
    assert metadata["max_volume"].value == 20.0
    assert metadata["totals"].value == {"rows": 3}

    min_only_metadata = helpers_mod._odds_sync_metadata(
        orch_config.OddsSyncConfig(min_volume=10.0, max_volume=None),
        {"planning": {}, "planning_context": {}, "totals": {}},
        {},
    )
    assert "min_volume" in min_only_metadata
    assert "max_volume" not in min_only_metadata

    max_only_metadata = helpers_mod._odds_sync_metadata(
        orch_config.OddsSyncConfig(min_volume=None, max_volume=20.0),
        {"planning": {}, "planning_context": {}, "totals": {}},
        {},
    )
    assert "min_volume" not in max_only_metadata
    assert "max_volume" in max_only_metadata


def test_run_with_guardrail_thread_success_and_error():
    class ImmediateThread:
        def __init__(self, target, daemon):
            self.target = target
            self.daemon = daemon
            self.started = False

        def start(self):
            self.started = True
            self.target()

        def is_alive(self):
            return False

        def join(self, timeout=None):
            del timeout

    guardrail = MagicMock()
    assert helpers_mod._run_with_guardrail_thread(
        guardrail,
        "phase",
        lambda: {"ok": True},
        poll_seconds=0,
        thread_factory=ImmediateThread,
    ) == {"ok": True}
    guardrail.record_progress.assert_called_with(
        work_increment=0,
        phase="phase_complete",
        diagnostics={"worker_alive": False},
        force_log=True,
    )

    def raise_boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        helpers_mod._run_with_guardrail_thread(
            guardrail,
            "phase",
            raise_boom,
            poll_seconds=0,
            thread_factory=ImmediateThread,
        )


def test_run_with_guardrail_thread_checks_while_worker_alive():
    class PollingThread:
        def __init__(self, target, daemon):
            self.target = target
            self.daemon = daemon
            self.alive_checks = 0

        def start(self):
            self.target()

        def is_alive(self):
            self.alive_checks += 1
            return self.alive_checks <= 2

        def join(self, timeout=None):
            del timeout

    guardrail = MagicMock()
    assert (
        helpers_mod._run_with_guardrail_thread(
            guardrail,
            "slow_phase",
            lambda: {},
            poll_seconds=0,
            thread_factory=PollingThread,
        )
        == {}
    )
    guardrail.check.assert_called_once_with(
        phase="slow_phase",
        diagnostics={"worker_alive": True},
    )


def test_odds_assets_delegate_to_materializer(monkeypatch):
    calls = []

    def materialize(_context, config, **kwargs):
        calls.append((type(config), kwargs))
        return "ok"

    monkeypatch.setattr(assets_mod.asset_helpers, "_materialize_odds_sync", materialize)

    ctx = MagicMock()
    assert (
        polymarket_wc2026_raw_token_odds_history_hourly.op.compute_fn.decorated_fn(
            ctx, orch_config.HourlyOddsSyncConfig()
        )
        == "ok"
    )
    assert calls[0][0] is orch_config.HourlyOddsSyncConfig
