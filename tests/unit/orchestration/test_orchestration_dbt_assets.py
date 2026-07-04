import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from pathlib import Path
from unittest.mock import MagicMock

import yaml

from oddsfox_pipeline.orchestration import config as orch_config
from oddsfox_pipeline.orchestration import dbt_build as dbt_build_mod
from tests.unit.orchestration.orchestration_test_support import (
    _DormantThread,
    _FakeClock,
    _FakeQueue,
    _ImmediateThread,
    _patch_guardrail_clock,
)


def test_dbt_source_metadata_maps_expected_dagster_asset_keys():
    sources_path = (
        Path(__file__).resolve().parents[3]
        / "dbt"
        / "models"
        / "sources"
        / "wc2026_polymarket_sources.yml"
    )
    data = yaml.safe_load(sources_path.read_text())
    tables = {
        (source["name"], table["name"]): table["meta"]["dagster"]["asset_key"]
        for source in data["sources"]
        for table in source["tables"]
    }

    assert tables[("wc2026_polymarket_raw", "markets")] == [
        "wc2026_polymarket_raw_markets"
    ]
    assert tables[("wc2026_polymarket_raw", "market_tokens")] == [
        "wc2026_polymarket_market_metadata_backfill"
    ]
    assert tables[("wc2026_polymarket_raw", "odds_history")] == [
        "wc2026_polymarket_token_odds_history_hourly"
    ]
    assert tables[("wc2026_polymarket_raw", "token_odds_daily")] == [
        "wc2026_polymarket_token_odds_history_hourly"
    ]
    assert tables[("wc2026_polymarket_ops", "token_sync_ledger")] == [
        "wc2026_polymarket_token_odds_history_hourly"
    ]
    assert tables[("wc2026_polymarket_ops", "token_sync_skips")] == [
        "wc2026_polymarket_token_odds_history_hourly"
    ]
    assert tables[("wc2026_polymarket_ops", "pipeline_run_events")] == [
        "wc2026_polymarket_token_odds_history_hourly"
    ]
    assert tables[("wc2026_polymarket_ops", "market_scope_registry")] == [
        "wc2026_polymarket_market_registry"
    ]


def test_dbt_translator_does_not_override_model_dependencies():
    from oddsfox_pipeline.orchestration.translators import (
        PolymarketDagsterDbtTranslator,
    )

    assert "get_asset_spec" not in PolymarketDagsterDbtTranslator.__dict__


def test_dbt_translator_resolves_source_deps_to_ingestion_assets():
    from dagster import AssetKey

    from oddsfox_pipeline.orchestration.definitions import defs

    graph = defs.resolve_asset_graph()
    stg_markets_parents = {
        key.to_user_string()
        for key in graph.get(AssetKey("wc2026_polymarket_stg_markets")).parent_keys
    }
    assert "wc2026_polymarket_raw_markets" in stg_markets_parents
    assert not any(parent.startswith("dbt_") for parent in stg_markets_parents)

    stg_odds_parents = {
        key.to_user_string()
        for key in graph.get(AssetKey("wc2026_polymarket_stg_odds")).parent_keys
    }
    assert "wc2026_polymarket_token_odds_history_hourly" in stg_odds_parents

    dangling_dbt_keys = sorted(
        key.to_user_string()
        for key in defs.resolve_all_asset_keys()
        if key.path[0].startswith("dbt_")
    )
    assert dangling_dbt_keys == []


def test_dbt_assets_definition_streams_build_events(monkeypatch):
    from oddsfox_pipeline.orchestration.assets import wc2026_polymarket_dbt

    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
        lambda: (_ for _ in ()).throw(AssertionError("dbt must not clean raw tables")),
    )

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock()
            m.process = MagicMock(returncode=0)
            m.stream = lambda: iter(["event"])
            return m

    fn = wc2026_polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    events = list(fn(ctx, MockDbt(), orch_config.DbtBuildConfig()))
    assert events == ["event"]


def test_dbt_assets_does_not_delete_orphan_market_tokens(monkeypatch):
    from oddsfox_pipeline.orchestration.assets import wc2026_polymarket_dbt

    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.polymarket_ops.delete_orphan_market_tokens",
        lambda: (_ for _ in ()).throw(AssertionError("dbt must not clean raw tables")),
    )

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock()
            m.process = MagicMock(returncode=0)
            m.stream = lambda: iter([])
            return m

    fn = wc2026_polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    list(fn(ctx, MockDbt(), orch_config.DbtBuildConfig()))


def test_dbt_assets_guardrail_hard_timeout_terminates_process(monkeypatch):
    from oddsfox_pipeline.orchestration import assets as assets_mod
    from oddsfox_pipeline.orchestration.assets import wc2026_polymarket_dbt

    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _DormantThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )

    process_mock = MagicMock(returncode=None)

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=process_mock)
            m.stream = lambda: iter(())
            return m

    fn = wc2026_polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    with pytest.raises(Exception):
        list(
            fn(
                ctx,
                MockDbt(),
                orch_config.DbtBuildConfig(
                    no_progress_soft_timeout_seconds=None,
                    no_progress_hard_timeout_seconds=1,
                    progress_log_interval_seconds=1,
                    progress_poll_seconds=1,
                ),
            )
        )
    assert process_mock.terminate.called


def test_dbt_assets_guardrail_wait_continue_and_stream_error(monkeypatch):
    from oddsfox_pipeline.orchestration import assets as assets_mod
    from oddsfox_pipeline.orchestration.assets import wc2026_polymarket_dbt

    fn = wc2026_polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )

    class SlowThenEventDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=None))
            m.stream = lambda: iter(["event"])
            return m

    events = list(
        fn(
            ctx,
            SlowThenEventDbt(),
            orch_config.DbtBuildConfig(
                no_progress_soft_timeout_seconds=None,
                no_progress_hard_timeout_seconds=None,
                progress_log_interval_seconds=1,
                progress_poll_seconds=1,
            ),
        )
    )
    assert events == ["event"]

    class ErrorStreamDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=1))

            def _stream():
                raise RuntimeError("dbt stream blew up")
                yield  # pragma: no cover

            m.stream = _stream
            return m

    with pytest.raises(RuntimeError, match="dbt stream blew up"):
        list(fn(MagicMock(), ErrorStreamDbt(), orch_config.DbtBuildConfig()))


def test_dbt_assets_raises_when_build_returns_nonzero_after_stream():
    from oddsfox_pipeline.orchestration.assets import wc2026_polymarket_dbt

    class NonZeroReturncodeDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=1))
            m.stream = lambda: iter(["event"])
            return m

    fn = wc2026_polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    with pytest.raises(RuntimeError, match="exit code 1"):
        list(fn(ctx, NonZeroReturncodeDbt(), orch_config.DbtBuildConfig()))


def test_prepare_dbt_project_warns_when_prepare_fails_but_manifest_exists(
    tmp_path, caplog
):
    import logging

    pytest.importorskip("dagster_dbt")

    from oddsfox_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    class FakePreparer:
        def using_dagster_dev(self):
            return True

        def prepare_if_dev(self, _project):
            raise RuntimeError("prepare failed")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    caplog.set_level(logging.WARNING)
    project = FakeProject()
    dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)
    assert any("prepare_if_dev() failed" in r.getMessage() for r in caplog.records)


def test_prepare_dbt_project_reraises_when_prepare_fails_and_manifest_missing(tmp_path):
    pytest.importorskip("dagster_dbt")

    from oddsfox_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "nonexistent_manifest.json"

    class FakePreparer:
        def using_dagster_dev(self):
            return True

        def prepare_if_dev(self, _project):
            raise RuntimeError("prepare failed")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    project = FakeProject()
    with pytest.raises(RuntimeError, match="prepare failed"):
        dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)


def test_prepare_dbt_project_prepares_manifest_outside_dagster_dev_when_missing(
    tmp_path,
):
    pytest.importorskip("dagster_dbt")

    from oddsfox_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    prepared: list[str] = []

    class FakePreparer:
        def using_dagster_dev(self):
            return False

        def prepare(self, project):
            prepared.append(str(project.manifest_path))
            manifest.write_text("{}")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    project = FakeProject()
    dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)
    assert prepared == [str(manifest)]
    assert manifest.exists()


def test_oddsfox_dbt_project_preparer_uses_resolved_executable(monkeypatch):
    pytest.importorskip("dagster_dbt")

    from oddsfox_pipeline.orchestration.dbt_project import OddsfoxDbtProjectPreparer

    captured: list[str] = []

    class FakeDbtCliResource:
        def __init__(self, **kwargs):
            captured.append(kwargs["dbt_executable"])

        def cli(self, *_args, **_kwargs):
            return self

        def wait(self):
            return None

    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.dbt_project.resolve_dbt_executable",
        lambda: "/venv/bin/dbt",
    )
    monkeypatch.setattr(
        "dagster_dbt.core.resource.DbtCliResource",
        FakeDbtCliResource,
    )

    preparer = OddsfoxDbtProjectPreparer()
    monkeypatch.setattr(
        preparer, "_invalidate_seeds_in_partial_parse", lambda _project: None
    )
    project = MagicMock(target_path=MagicMock(), profiles_dir="dbt/profiles")
    preparer._prepare_packages(project)
    preparer._prepare_manifest(project)
    assert captured == ["/venv/bin/dbt", "/venv/bin/dbt"]


def test_stream_dbt_build_appends_full_refresh_flag():
    from unittest.mock import MagicMock

    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            m = MagicMock()
            m.stream = lambda: iter(["event"])
            m.process = MagicMock(returncode=0)
            return m

    ctx = MagicMock()
    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="wc2026_polymarket_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(full_refresh=True),
        )
    )
    assert captured_args == [["build", "--full-refresh"]]


def test_stream_dbt_build_merges_heartbeat_diagnostics(monkeypatch):
    from oddsfox_pipeline.orchestration import assets as assets_mod

    ctx = MagicMock()
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )
    heartbeat_calls = []

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=0))
            m.stream = lambda: iter([])
            return m

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="wc2026_polymarket_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                no_progress_soft_timeout_seconds=None,
                no_progress_hard_timeout_seconds=None,
                progress_log_interval_seconds=1,
                progress_poll_seconds=1,
            ),
            heartbeat_diagnostics_fn=lambda: (
                heartbeat_calls.append(True) or {"heartbeat": "ok"}
            ),
        )
    )

    assert heartbeat_calls == [True]


def test_stream_dbt_build_ignores_non_dict_heartbeat(monkeypatch):
    from oddsfox_pipeline.orchestration import assets as assets_mod

    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=0))
            m.stream = lambda: iter([])
            return m

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="wc2026_polymarket_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                no_progress_soft_timeout_seconds=None,
                no_progress_hard_timeout_seconds=None,
                progress_log_interval_seconds=1,
                progress_poll_seconds=1,
            ),
            heartbeat_diagnostics_fn=lambda: None,
        )
    )
