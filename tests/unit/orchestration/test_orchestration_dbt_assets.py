import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

import os
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
    sources_root = Path(__file__).resolve().parents[3] / "dbt" / "models" / "sources"
    tables = {}
    for source_file in (
        "polymarket_wc2026_sources.yml",
        "international_results_wc2026_sources.yml",
        "openfootball_wc2026_sources.yml",
    ):
        data = yaml.safe_load((sources_root / source_file).read_text())
        tables.update(
            {
                (source["name"], table["name"]): table["meta"]["dagster"]["asset_key"]
                for source in data["sources"]
                for table in source["tables"]
            }
        )

    assert tables[("polymarket_wc2026_raw", "markets")] == [
        "polymarket",
        "wc2026",
        "raw",
        "markets",
    ]
    assert tables[("polymarket_wc2026_raw", "market_tokens")] == [
        "polymarket",
        "wc2026",
        "raw",
        "market_metadata_backfill",
    ]
    assert tables[("polymarket_wc2026_raw", "odds_history")] == [
        "polymarket",
        "wc2026",
        "raw",
        "token_odds_history_hourly",
    ]
    assert tables[("polymarket_wc2026_raw", "match_minute_odds_history")] == [
        "polymarket",
        "wc2026",
        "raw",
        "match_token_odds_history_minute",
    ]
    assert tables[("polymarket_wc2026_ops", "match_minute_odds_fetch_audit")] == [
        "polymarket",
        "wc2026",
        "raw",
        "match_token_odds_history_minute",
    ]
    assert tables[("polymarket_wc2026_raw", "token_odds_daily")] == [
        "polymarket",
        "wc2026",
        "raw",
        "token_odds_history_hourly",
    ]
    assert tables[("polymarket_wc2026_ops", "token_sync_ledger")] == [
        "polymarket",
        "wc2026",
        "raw",
        "token_odds_history_hourly",
    ]
    assert tables[("polymarket_wc2026_ops", "token_sync_skips")] == [
        "polymarket",
        "wc2026",
        "raw",
        "token_odds_history_hourly",
    ]
    assert tables[("polymarket_wc2026_ops", "pipeline_run_events")] == [
        "polymarket",
        "wc2026",
        "raw",
        "token_odds_history_hourly",
    ]
    assert tables[("polymarket_wc2026_ops", "market_scope_registry")] == [
        "polymarket",
        "wc2026",
        "ops",
        "market_scope_registry",
    ]
    assert tables[("international_results_wc2026_raw", "match_results")] == [
        "international_results",
        "wc2026",
        "raw",
        "match_results",
    ]
    assert tables[("international_results_wc2026_raw", "historical_matches")] == [
        "international_results",
        "historical",
        "raw",
        "snapshot",
    ]
    assert tables[("openfootball_wc2026_raw", "knockout_fixtures")] == [
        "openfootball",
        "wc2026",
        "raw",
        "knockout_fixtures",
    ]


def test_dbt_translator_does_not_override_model_dependencies():
    from oddsfox_pipeline.orchestration.translators import (
        PolymarketDagsterDbtTranslator,
    )

    assert "get_asset_spec" not in PolymarketDagsterDbtTranslator.__dict__


def test_dbt_translator_enables_source_visibility_settings():
    from oddsfox_pipeline.orchestration.translators import (
        PolymarketDagsterDbtTranslator,
    )

    settings = PolymarketDagsterDbtTranslator().settings

    assert settings.enable_duplicate_source_asset_keys is True
    assert settings.enable_source_metadata is True
    assert settings.enable_source_tests_as_checks is True


def test_dbt_translator_resolves_source_deps_to_ingestion_assets():
    from dagster import AssetKey

    from oddsfox_pipeline.orchestration.definitions import defs

    graph = defs.resolve_asset_graph()
    stg_markets_parents = {
        key.to_user_string()
        for key in graph.get(
            AssetKey(["polymarket", "wc2026", "staging", "markets"])
        ).parent_keys
    }
    assert "polymarket/wc2026/raw/markets" in stg_markets_parents
    assert not any(parent.startswith("dbt_") for parent in stg_markets_parents)

    stg_odds_parents = {
        key.to_user_string()
        for key in graph.get(
            AssetKey(["polymarket", "wc2026", "staging", "odds"])
        ).parent_keys
    }
    assert "polymarket/wc2026/raw/token_odds_history_hourly" in stg_odds_parents

    stg_results_parents = {
        key.to_user_string()
        for key in graph.get(
            AssetKey(["international_results", "wc2026", "staging", "match_results"])
        ).parent_keys
    }
    assert "international_results/wc2026/raw/match_results" in stg_results_parents

    stg_fixtures_parents = {
        key.to_user_string()
        for key in graph.get(
            AssetKey(["openfootball", "wc2026", "staging", "knockout_fixtures"])
        ).parent_keys
    }
    assert "openfootball/wc2026/raw/knockout_fixtures" in stg_fixtures_parents

    dangling_dbt_keys = sorted(
        key.to_user_string()
        for key in defs.resolve_all_asset_keys()
        if key.path[0].startswith("dbt_")
    )
    assert dangling_dbt_keys == []


def test_dbt_assets_definition_streams_build_events(monkeypatch):
    from oddsfox_pipeline.orchestration.assets import oddsfox_dbt

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

    fn = oddsfox_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    events = list(fn(ctx, MockDbt(), orch_config.DbtBuildConfig()))
    assert events == ["event"]


def test_match_minute_asset_materializes_sync_summary(monkeypatch):
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_match_token_odds_history_minute,
    )

    connection = MagicMock()
    connection.__enter__.return_value = "connection"
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.assets_polymarket.get_connection",
        lambda: connection,
    )
    sync = MagicMock(return_value={"games": 104, "markets": 248, "tokens": 496})
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.polymarket_ops.sync_match_minute_odds_history",
        sync,
    )
    save_metrics = MagicMock()
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.assets_polymarket.save_sync_run_metrics",
        save_metrics,
    )

    context = MagicMock()
    config = orch_config.MatchMinuteOddsSyncConfig()
    result = polymarket_wc2026_raw_match_token_odds_history_minute.op.compute_fn.decorated_fn(
        context, config
    )

    sync.assert_called_once_with(
        "connection",
        log=context.log,
        workers=config.workers,
        requests_per_second=config.requests_per_second,
        transient_retries=config.transient_retries,
        transient_backoff_seconds=config.transient_backoff_seconds,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
    )
    save_metrics.assert_called_once_with(
        "match_minute_odds",
        {"games": 104, "markets": 248, "tokens": 496},
        scope_name="wc2026",
    )
    assert result.metadata["tokens"] == 496


def test_match_minute_asset_records_failure_summary(monkeypatch):
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_match_token_odds_history_minute,
    )

    connection = MagicMock()
    connection.__enter__.return_value = "connection"
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.assets_polymarket.get_connection",
        lambda: connection,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.polymarket_ops.sync_match_minute_odds_history",
        MagicMock(side_effect=RuntimeError("preflight failed")),
    )
    save_metrics = MagicMock()
    monkeypatch.setattr(
        "oddsfox_pipeline.orchestration.assets_polymarket.save_sync_run_metrics",
        save_metrics,
    )

    with pytest.raises(RuntimeError, match="preflight failed"):
        polymarket_wc2026_raw_match_token_odds_history_minute.op.compute_fn.decorated_fn(
            MagicMock(), orch_config.MatchMinuteOddsSyncConfig()
        )

    save_metrics.assert_called_once_with(
        "match_minute_odds",
        {"status": "preflight_error", "error_type": "RuntimeError"},
        scope_name="wc2026",
    )


def test_dbt_assets_does_not_delete_orphan_market_tokens(monkeypatch):
    from oddsfox_pipeline.orchestration.assets import oddsfox_dbt

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

    fn = oddsfox_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    list(fn(ctx, MockDbt(), orch_config.DbtBuildConfig()))


def test_dbt_assets_guardrail_hard_timeout_terminates_process(monkeypatch):
    from oddsfox_pipeline.orchestration import assets as assets_mod
    from oddsfox_pipeline.orchestration.assets import oddsfox_dbt

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

    fn = oddsfox_dbt.op.compute_fn.decorated_fn
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
    from oddsfox_pipeline.orchestration.assets import oddsfox_dbt

    fn = oddsfox_dbt.op.compute_fn.decorated_fn
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
    from oddsfox_pipeline.orchestration.assets import oddsfox_dbt

    class NonZeroReturncodeDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=1))
            m.stream = lambda: iter(["event"])
            return m

    fn = oddsfox_dbt.op.compute_fn.decorated_fn
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


def test_prepare_dbt_project_skips_prepare_when_manifest_exists_outside_dev(tmp_path):
    pytest.importorskip("dagster_dbt")

    from oddsfox_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    class FakePreparer:
        def using_dagster_dev(self):
            return False

        def prepare(self, _project):
            raise AssertionError("existing manifests should not be prepared")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    project = FakeProject()
    dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)


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
            asset_name="oddsfox_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(full_refresh=True),
        )
    )
    assert captured_args == [
        ["build", "--full-refresh", "--exclude", "tag:polygon_settlement"]
    ]


def test_stream_dbt_build_appends_dbt_exclude_flag():
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
            asset_name="oddsfox_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(dbt_exclude="tag:cross_domain"),
        )
    )
    assert captured_args == [["build", "--exclude", "tag:cross_domain"]]


def test_stream_dbt_build_omits_empty_exclude_for_full_build():
    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            invocation = MagicMock()
            invocation.stream = lambda: iter(())
            invocation.process = MagicMock(returncode=0)
            return invocation

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=MagicMock(is_subset=False),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(dbt_exclude=None),
        )
    )

    assert captured_args == [["build"]]


def test_stream_dbt_build_appends_dbt_select_before_exclude_flags():
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
            asset_name="oddsfox_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                full_refresh=True,
                dbt_select="+tag:kalshi",
                dbt_exclude="tag:cross_domain tag:polymarket",
            ),
        )
    )
    assert captured_args == [
        [
            "build",
            "--full-refresh",
            "--select",
            "+tag:kalshi",
            "--exclude",
            "tag:cross_domain tag:polymarket",
        ]
    ]


def test_stream_dbt_build_does_not_union_config_selectors_into_subset():
    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            invocation = MagicMock()
            invocation.stream = lambda: iter(())
            invocation.process = MagicMock(returncode=0)
            return invocation

    context = MagicMock(is_subset=True)
    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=context,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                dbt_select="+tag:cross_domain",
                dbt_exclude="tag:unrelated",
            ),
        )
    )

    assert captured_args == [["build"]]


def test_stream_dbt_build_keeps_polygon_graph_opt_in_for_subset():
    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            invocation = MagicMock()
            invocation.stream = lambda: iter(())
            invocation.process = MagicMock(returncode=0)
            return invocation

    context = MagicMock(is_subset=True)
    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=context,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(),
        )
    )

    assert captured_args == [["build", "--exclude", "tag:polygon_settlement"]]


def test_stream_dbt_build_fetches_row_counts_and_column_metadata():
    from unittest.mock import MagicMock

    calls: list[object] = []

    class FakeDbtEventStream:
        def fetch_row_counts(self):
            calls.append("row_counts")
            return self

        def fetch_column_metadata(self, *, with_column_lineage=True):
            calls.append(("column_metadata", with_column_lineage))
            return self

        def __iter__(self):
            yield "event"

    class MockDbt:
        def cli(self, args, context=None):
            m = MagicMock()
            m.adapter.cleanup_connections = lambda: calls.append("cleanup")
            m.stream = lambda: FakeDbtEventStream()
            m.process = MagicMock(returncode=0)
            return m

    ctx = MagicMock()
    events = list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(fetch_dbt_metadata=True),
        )
    )
    assert events == ["event"]
    assert calls == ["row_counts", ("column_metadata", False), "cleanup"]


def test_stream_dbt_build_skips_dbt_metadata_fetch_by_default():
    from unittest.mock import MagicMock

    class FakeDbtEventStream:
        def fetch_row_counts(self):
            raise AssertionError("row counts should be opt-in")

        def fetch_column_metadata(self, *, with_column_lineage=True):
            raise AssertionError("column metadata should be opt-in")

        def __iter__(self):
            yield "event"

    class MockDbt:
        def cli(self, args, context=None):
            m = MagicMock()
            m.stream = lambda: FakeDbtEventStream()
            m.process = MagicMock(returncode=0)
            return m

    ctx = MagicMock()
    events = list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(),
        )
    )
    assert events == ["event"]


def test_stream_dbt_build_handles_missing_opt_in_dbt_metadata_hooks():
    class MockDbt:
        def cli(self, args, context=None):
            m = MagicMock()
            m.stream = lambda: iter(["event"])
            m.process = MagicMock(returncode=0)
            return m

    events = list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(fetch_dbt_metadata=True),
        )
    )
    assert events == ["event"]


def test_cleanup_dbt_adapter_handles_adapter_shapes():
    calls: list[str] = []

    dbt_build_mod._cleanup_dbt_adapter(MagicMock(adapter=None))

    adapter = MagicMock()
    adapter.cleanup_connections.side_effect = lambda: calls.append("connections")
    adapter.connections.cleanup_all.side_effect = lambda: calls.append("all")
    dbt_build_mod._cleanup_dbt_adapter(MagicMock(adapter=adapter))
    assert calls == ["connections", "all"]


def test_stream_dbt_build_syncs_duckdb_path_env(monkeypatch, tmp_path):
    from unittest.mock import MagicMock

    db_path = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setattr(
        dbt_build_mod,
        "active_duckdb_path",
        lambda: db_path,
    )
    monkeypatch.setattr(dbt_build_mod, "ensure_duck_db", lambda: None)

    class MockDbt:
        def cli(self, args, context=None):
            m = MagicMock()
            m.stream = lambda: iter([])
            m.process = MagicMock(returncode=0)
            return m

    ctx = MagicMock()
    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(),
        )
    )
    assert os.environ["DUCKDB_PATH"] == str(db_path)


def test_stream_dbt_build_checks_disposable_path_before_initializing(monkeypatch):
    calls: list[tuple[str, object]] = []

    def reject(path):
        calls.append(("guard", path))
        raise RuntimeError("unsafe warehouse")

    monkeypatch.setattr(dbt_build_mod, "assert_disposable_duckdb_path", reject)
    monkeypatch.setattr(
        dbt_build_mod,
        "ensure_duck_db",
        lambda: calls.append(("ensure", None)),
    )

    with pytest.raises(RuntimeError, match="unsafe warehouse"):
        list(
            dbt_build_mod.stream_dbt_build(
                asset_name="oddsfox_dbt",
                context=MagicMock(),
                dbt=MagicMock(),
                config=orch_config.DbtBuildConfig(
                    expected_duckdb_path=".cache/polygon-smoke.duckdb"
                ),
            )
        )

    assert calls == [("guard", ".cache/polygon-smoke.duckdb")]


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
            asset_name="oddsfox_dbt",
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
            asset_name="oddsfox_dbt",
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
