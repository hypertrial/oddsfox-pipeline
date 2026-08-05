import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from unittest.mock import MagicMock, patch

from oddsfox_pipeline.orchestration import polymarket_ops as polymarket_ops_mod
from tests.unit.orchestration.orchestration_test_support import (
    _FakeClock,
    _patch_guardrail_clock,
)


def test_metadata_enrichment_asset_invokes_progress_callback(monkeypatch):
    from dagster import materialize

    from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_market_metadata_enrichment,
    )

    op_key = polymarket_wc2026_raw_market_metadata_enrichment.op.name
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    def combined(**kw):
        cb = kw.get("progress_callback")
        if cb:
            cb("probe_metadata", {"x": 1})
        return {"task": "enrich_market_metadata", "skipped": True}

    with (
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.enrich_market_metadata",
            combined,
        ),
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
            lambda **_kwargs: 0,
        ),
    ):
        materialize(
            [polymarket_wc2026_raw_market_metadata_enrichment],
            run_config={
                "ops": {
                    op_key: {
                        "config": {
                            "include_slugs": True,
                            "include_event_slugs": True,
                            "include_end_dates": True,
                        }
                    }
                }
            },
        )


def test_metadata_enrichment_config_branches(monkeypatch):
    from dagster import materialize

    from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_market_metadata_enrichment,
    )

    op_key = polymarket_wc2026_raw_market_metadata_enrichment.op.name
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})
    cfg = {
        "batch_size": 20,
        "force": False,
        "include_slugs": False,
        "include_event_slugs": False,
        "include_end_dates": False,
    }
    with (
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.enrich_market_metadata",
            lambda **k: {"task": "enrich_market_metadata", "skipped": True},
        ),
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
            lambda **_kwargs: 0,
        ),
    ):
        materialize(
            [polymarket_wc2026_raw_market_metadata_enrichment],
            run_config={"ops": {op_key: {"config": cfg}}},
        )

    cfg2 = {
        **cfg,
        "include_slugs": True,
        "include_event_slugs": False,
        "include_end_dates": True,
    }
    with (
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.enrich_market_metadata",
            lambda **k: {"task": "enrich_market_metadata", "skipped": True},
        ),
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
            lambda **_kwargs: 0,
        ),
    ):
        materialize(
            [polymarket_wc2026_raw_market_metadata_enrichment],
            run_config={"ops": {op_key: {"config": cfg2}}},
        )


def test_metadata_enrichment_forwards_event_slug_fallback_and_gamma_kwargs(
    monkeypatch,
):
    from dagster import materialize

    from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_market_metadata_enrichment,
    )

    op_key = polymarket_wc2026_raw_market_metadata_enrichment.op.name
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})
    captured = {}

    def capture_metadata(**kw):
        captured.update(kw)
        return {"task": "enrich_market_metadata", "skipped": True}

    with (
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.enrich_market_metadata",
            capture_metadata,
        ),
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
            lambda **_kwargs: 0,
        ),
    ):
        materialize(
            [polymarket_wc2026_raw_market_metadata_enrichment],
            run_config={
                "ops": {
                    op_key: {
                        "config": {
                            "include_slugs": True,
                            "include_event_slugs": True,
                            "include_end_dates": False,
                            "gamma_requests_per_second": 1.5,
                            "event_slug_fallback_max_pages": 42,
                            "event_slug_fallback_max_pages_without_progress": 5,
                            "event_slug_fallback_progress_pages": 7,
                            "progress_log_interval_batches": 3,
                        }
                    }
                }
            },
        )

    assert captured["gamma_requests_per_second"] == 1.5
    assert captured["progress_every_n_batches"] == 3
    assert captured["batch_size"] == 50
    assert captured["event_slug_fallback_max_pages"] == 42
    assert captured["event_slug_fallback_max_pages_without_progress"] == 5
    assert captured["event_slug_fallback_progress_every_pages"] == 7
    assert captured["include_event_slugs"] is True
    assert captured["include_end_dates"] is False
    assert captured["market_scope"] == "wc2026"
    assert callable(captured["progress_callback"])


def test_metadata_enrichment_deletes_orphan_market_tokens_after_backfill(monkeypatch):
    from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
    from oddsfox_pipeline.orchestration import config as orch_config
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_market_metadata_enrichment,
    )

    calls: list[str] = []
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})
    monkeypatch.setattr(
        polymarket_ops_mod,
        "enrich_market_metadata",
        lambda **_kwargs: calls.append("backfill") or {"task": "metadata"},
    )
    monkeypatch.setattr(
        polymarket_ops_mod,
        "delete_orphan_market_tokens",
        lambda **_kwargs: calls.append("cleanup") or 2,
    )

    fn = polymarket_wc2026_raw_market_metadata_enrichment.op.compute_fn.decorated_fn
    ctx = MagicMock()
    result = fn(ctx, orch_config.MetadataEnrichmentConfig())

    assert calls == ["backfill", "cleanup"]
    assert result.metadata["orphan_market_tokens_removed"].value == 2
    joined = " ".join(str(c) for c in ctx.log.info.call_args_list)
    assert "orphan market_tokens" in joined


def test_metadata_enrichment_guardrail_poll_checks_and_raises_worker_errors(
    monkeypatch,
):
    from dagster import materialize

    from oddsfox_pipeline.orchestration import assets as assets_mod
    from oddsfox_pipeline.orchestration import assets_polymarket as polymarket_assets
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_market_metadata_enrichment,
    )

    op_key = polymarket_wc2026_raw_market_metadata_enrichment.op.name
    check_calls = {"count": 0}
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    monkeypatch.setattr(polymarket_assets, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(polymarket_assets, "delta_raw_layer", lambda _pre, _post: {})
    real_check = polymarket_ops_mod.ProgressGuardrail.check

    def counting_check(self, *args, **kwargs):
        check_calls["count"] += 1
        return real_check(self, *args, **kwargs)

    monkeypatch.setattr(polymarket_ops_mod.ProgressGuardrail, "check", counting_check)

    def enrich_with_progress(**kwargs):
        progress = kwargs.get("progress_callback")
        if progress is not None:
            progress("enrich_batch", {"batch": 1})
        return {"task": "enrich_market_metadata", "ok": True}

    with (
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.enrich_market_metadata",
            enrich_with_progress,
        ),
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
            lambda **_kwargs: 0,
        ),
    ):
        materialize(
            [polymarket_wc2026_raw_market_metadata_enrichment],
            run_config={
                "ops": {
                    op_key: {
                        "config": {
                            "include_slugs": False,
                            "include_event_slugs": False,
                            "include_end_dates": False,
                            "progress_poll_seconds": 1,
                            "no_progress_soft_timeout_seconds": None,
                            "no_progress_hard_timeout_seconds": None,
                        }
                    }
                }
            },
        )

    assert check_calls["count"] >= 1

    with (
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.enrich_market_metadata",
            lambda **k: (_ for _ in ()).throw(RuntimeError("boom phase")),
        ),
        patch(
            "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
            lambda **_kwargs: 0,
        ),
    ):
        with pytest.raises(RuntimeError, match="boom phase"):
            materialize(
                [polymarket_wc2026_raw_market_metadata_enrichment],
                run_config={
                    "ops": {
                        op_key: {
                            "config": {
                                "include_slugs": False,
                                "include_event_slugs": False,
                                "include_end_dates": False,
                            }
                        }
                    }
                },
            )


def test_market_scope_registry_helper_persists_failure_metrics(monkeypatch):
    from oddsfox_pipeline.orchestration import config as orch_config
    from oddsfox_pipeline.orchestration import (
        polymarket_asset_helpers_registry as helpers,
    )

    failures = []
    monkeypatch.setattr(
        helpers,
        "save_asset_failure_metrics",
        lambda task, exc, **kwargs: failures.append((task, str(exc), kwargs)),
    )
    monkeypatch.setattr(
        helpers,
        "_run_with_raw_snapshot",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        helpers._materialize_market_scope_registry(
            MagicMock(log=MagicMock()),
            orch_config.MarketScopeRegistryConfig(force_refresh=True),
            scope_name="wc2026",
            get_sync_run_metrics_fn=lambda *_a, **_k: None,
            snapshot_refreshed_scope_name_fn=lambda _metrics: None,
            sync_market_scope_registry_fn=lambda **_k: {},
        )

    assert failures == [
        (
            "sync_market_scope_registry",
            "boom",
            {"scope_name": "wc2026"},
        )
    ]


def test_event_catalog_helper_exercises_checkpoint_callbacks(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from dagster import AssetKey

    from oddsfox_pipeline.orchestration import config as orch_config
    from oddsfox_pipeline.orchestration import (
        polymarket_asset_helpers_registry as helpers,
    )

    calls = []
    conn = object()

    @contextmanager
    def connection():
        yield conn

    monkeypatch.setattr(
        helpers,
        "load_event_catalog_partition_checkpoints",
        lambda active, **kwargs: calls.append(("load", active, kwargs)) or {},
    )
    monkeypatch.setattr(
        helpers,
        "save_event_catalog_partition_checkpoint",
        lambda active, *args, **kwargs: calls.append(("save", active, args, kwargs)),
    )
    monkeypatch.setattr(
        helpers,
        "clear_event_catalog_partition_checkpoints",
        lambda active, **kwargs: calls.append(("clear", active, kwargs)),
    )

    def collect(**kwargs):
        kwargs["progress_callback"]("page", {"events_page": 2})
        kwargs["load_checkpoint_fn"]()
        kwargs["save_checkpoint_fn"]("partition", {"e": {}}, {"complete": True})
        return SimpleNamespace(
            market_payloads=[{"id": "m"}],
            event_snapshots=[{"id": "e"}],
            event_tag_snapshots=[{"id": "tag"}],
            event_market_snapshots=[{"id": "membership"}],
            summary={"observed_at": "2026-01-01", "events": 1},
        )

    results = list(
        helpers._materialize_event_catalog(
            MagicMock(log=MagicMock()),
            orch_config.MarketScopeRegistryConfig(reset_event_catalog_checkpoint=True),
            asset_name="event_catalog",
            scope_name="wc2026",
            collect_event_catalog_fn=collect,
            merge_event_catalog_batch_fn=lambda **_kwargs: None,
            normalize_market_payloads_fn=lambda rows, **_kwargs: rows,
            ensure_indexes_fn=lambda *_a, **_k: None,
            get_connection_fn=connection,
            save_sync_run_metrics_fn=lambda *_a, **_k: None,
            event_catalog_key=AssetKey("catalog"),
            event_snapshots_key=AssetKey("snapshots"),
            event_memberships_key=AssetKey("memberships"),
        )
    )

    assert len(results) == 3
    assert [call[0] for call in calls] == ["clear", "load", "save", "clear"]


def test_raw_markets_helper_persists_failure_metrics(monkeypatch):
    from oddsfox_pipeline.orchestration import config as orch_config
    from oddsfox_pipeline.orchestration import (
        polymarket_asset_helpers_markets as helpers,
    )

    failures = []
    monkeypatch.setattr(
        helpers, "get_polymarket_dlt_pipeline", lambda **_kwargs: MagicMock()
    )
    monkeypatch.setattr(
        helpers,
        "save_asset_failure_metrics",
        lambda task, exc, **kwargs: failures.append((task, str(exc), kwargs)),
    )

    with pytest.raises(RuntimeError, match="boom"):
        list(
            helpers._run_raw_markets(
                MagicMock(log=MagicMock()),
                orch_config.MarketsSyncConfig(),
                MagicMock(),
                asset_name="polymarket_wc2026_raw_markets",
                scope_name="wc2026",
                discovery_mode="full_keyset",
                source_fn=lambda **_kwargs: object(),
                collect_market_scope_payload_fn=lambda **_kwargs: (_ for _ in ()).throw(
                    RuntimeError("boom")
                ),
                save_market_tokens_batch_fn=lambda *_a, **_k: None,
                save_sync_run_metrics_fn=lambda *_a, **_k: None,
                get_connection_fn=MagicMock(),
                ensure_indexes_fn=lambda *_a, **_k: None,
            )
        )

    assert failures == [("sync_markets", "boom", {"scope_name": "wc2026"})]


def test_raw_markets_helper_records_collection_progress(monkeypatch):
    from contextlib import contextmanager

    from oddsfox_pipeline.orchestration import config as orch_config
    from oddsfox_pipeline.orchestration import (
        polymarket_asset_helpers_markets as helpers,
    )

    pipeline = MagicMock(has_pending_data=False)
    monkeypatch.setattr(
        helpers, "get_polymarket_dlt_pipeline", lambda **_kwargs: pipeline
    )

    @contextmanager
    def connection():
        yield object()

    def collect(**kwargs):
        kwargs["progress_callback"]("events", {"events_page": 2})
        return {
            "market_rows": [],
            "token_rows": [],
            "run_summary": {"total_fetched": 0},
        }

    dlt_resource = MagicMock()
    dlt_resource.run.return_value = iter(())
    list(
        helpers._run_raw_markets(
            MagicMock(log=MagicMock()),
            orch_config.MarketsSyncConfig(),
            dlt_resource,
            asset_name="polymarket_wc2026_raw_markets",
            scope_name="wc2026",
            discovery_mode="full_keyset",
            source_fn=lambda **_kwargs: object(),
            collect_market_scope_payload_fn=collect,
            save_market_tokens_batch_fn=lambda *_a, **_k: None,
            save_sync_run_metrics_fn=lambda *_a, **_k: None,
            get_connection_fn=connection,
            ensure_indexes_fn=lambda *_a, **_k: None,
        )
    )

    pipeline.drop_pending_packages.assert_not_called()
