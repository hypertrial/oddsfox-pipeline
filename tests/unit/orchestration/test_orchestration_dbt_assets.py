import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
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
        "market_metadata_enrichment",
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
    for relation in (
        ("polymarket_wc2026_raw", "match_order_book_snapshots"),
        ("polymarket_wc2026_ops", "match_order_book_scan_runs"),
        ("polymarket_wc2026_ops", "match_order_book_scan_windows"),
    ):
        assert tables[relation] == [
            "polymarket",
            "wc2026",
            "raw",
            "match_order_book_snapshots",
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
    assert tables[("polymarket_wc2026_ops", "ingestion_run_events")] == [
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
    assert tables[("openfootball_wc2026_raw", "schedule_fixtures")] == [
        "openfootball",
        "wc2026",
        "raw",
        "schedule_fixtures",
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
    assert "polymarket/wc2026/raw/event_catalog" in stg_markets_parents
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
            AssetKey(["openfootball", "wc2026", "staging", "schedule_fixtures"])
        ).parent_keys
    }
    assert "openfootball/wc2026/raw/schedule_fixtures" in stg_fixtures_parents

    stg_order_book_parents = {
        key.to_user_string()
        for key in graph.get(
            AssetKey(
                [
                    "polymarket",
                    "wc2026",
                    "staging",
                    "match_order_book_snapshots",
                ]
            )
        ).parent_keys
    }
    assert "polymarket/wc2026/raw/match_order_book_snapshots" in (
        stg_order_book_parents
    )

    dangling_dbt_keys = sorted(
        key.to_user_string()
        for key in defs.resolve_all_asset_keys()
        if key.path[0].startswith("dbt_")
        and "us_midterms_2026" not in key.to_user_string()
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
    from oddsfox_pipeline.orchestration import assets_polymarket
    from oddsfox_pipeline.orchestration.assets import (
        polymarket_wc2026_raw_match_token_odds_history_minute,
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
        connection_factory=assets_polymarket.get_connection,
        log=context.log,
        workers=config.workers,
        requests_per_second=config.requests_per_second,
        batch_group_size=config.batch_group_size,
        window_hours=config.window_hours,
        auto_tune_rps=config.auto_tune_rps,
        auto_tune_max_rps=config.auto_tune_max_rps,
        transient_retries=config.transient_retries,
        transient_backoff_seconds=config.transient_backoff_seconds,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        market_sample_fraction=None,
        market_sample_seed=None,
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


def test_dbt_assets_guardrail_hard_timeout_escalates_to_sigkill(monkeypatch):
    """SIGTERM alone can leave a busy DuckDB query's dbt process alive and
    holding the warehouse lock; the guardrail must escalate to SIGKILL."""
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

    process_mock = MagicMock(returncode=None, pid=4242)
    process_mock.wait.side_effect = subprocess.TimeoutExpired(cmd="dbt", timeout=30)

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
    assert process_mock.kill.called
    # `wait()` keeps raising TimeoutExpired even after SIGKILL (e.g. a thread
    # wedged in uninterruptible I/O); that must be surfaced, not swallowed.
    error_messages = [call.args[0] for call in ctx.log.error.call_args_list]
    assert any("still alive" in msg for msg in error_messages)


def test_stream_dbt_build_terminates_on_generator_close(monkeypatch):
    """Dagster cancellation closes the generator; the dbt child must not orphan."""
    process_mock = MagicMock(returncode=None, pid=5151)
    process_mock.poll.return_value = None
    process_mock.wait.return_value = 0

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=process_mock)
            m.stream = lambda: iter(["event-1", "event-2"])
            return m

    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    gen = dbt_build_mod.stream_dbt_build(
        asset_name="oddsfox_dbt",
        context=MagicMock(),
        dbt=MockDbt(),
        config=orch_config.DbtBuildConfig(
            no_progress_soft_timeout_seconds=None,
            no_progress_hard_timeout_seconds=None,
        ),
    )
    assert next(gen) == "event-1"
    gen.close()
    assert process_mock.terminate.called


def test_stream_dbt_build_success_does_not_signal_finished_process(monkeypatch):
    process_mock = MagicMock(returncode=0, pid=6161)
    process_mock.poll.return_value = 0

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=process_mock)
            m.stream = lambda: iter(["event"])
            return m

    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    events = list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                no_progress_soft_timeout_seconds=None,
                no_progress_hard_timeout_seconds=None,
            ),
        )
    )
    assert events == ["event"]
    assert not process_mock.terminate.called
    assert not process_mock.kill.called


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


def test_prepare_dbt_project_prepares_stale_manifest_outside_dev(tmp_path):
    pytest.importorskip("dagster_dbt")

    from oddsfox_pipeline.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    project_dir = tmp_path / "dbt"
    (project_dir / "models").mkdir(parents=True)
    model = project_dir / "models" / "model.sql"
    model.write_text("select 1\n")
    manifest.write_text("{}")
    os.utime(manifest, (1000, 1000))
    os.utime(model, (1100, 1100))
    prepared: list[str] = []

    class FakePreparer:
        def using_dagster_dev(self):
            return False

        def prepare(self, project):
            prepared.append(str(project.manifest_path))

    project = SimpleNamespace(
        project_dir=project_dir,
        manifest_path=manifest,
        preparer=FakePreparer(),
    )
    dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)
    assert prepared == [str(manifest)]


def _mini_dbt_project(tmp_path: Path) -> tuple[Path, Path, Path]:
    project_dir = tmp_path / "dbt"
    models = project_dir / "models"
    models.mkdir(parents=True)
    model = models / "example.sql"
    model.write_text("select 1 as id\n", encoding="utf-8")
    (project_dir / "dbt_project.yml").write_text("name: demo\n", encoding="utf-8")
    manifest = project_dir / "target" / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text('{"nodes": {}}\n', encoding="utf-8")
    return project_dir, model, manifest


def test_dbt_manifest_inputs_stale_when_model_newer(tmp_path):
    from oddsfox_pipeline.orchestration.dbt_project import dbt_manifest_inputs_stale

    project_dir, model, manifest = _mini_dbt_project(tmp_path)
    os.utime(manifest, (1000, 1000))
    os.utime(project_dir / "dbt_project.yml", (900, 900))
    os.utime(model, (1100, 1100))
    assert dbt_manifest_inputs_stale(project_dir, manifest) is True


def test_dbt_manifest_inputs_stale_false_when_manifest_fresh(tmp_path):
    from oddsfox_pipeline.orchestration.dbt_project import dbt_manifest_inputs_stale

    project_dir, model, manifest = _mini_dbt_project(tmp_path)
    os.utime(model, (900, 900))
    os.utime(project_dir / "dbt_project.yml", (900, 900))
    os.utime(manifest, (1100, 1100))
    assert dbt_manifest_inputs_stale(project_dir, manifest) is False


def test_prepare_dbt_project_skips_fresh_manifest_outside_dev(tmp_path):
    from oddsfox_pipeline.orchestration import dbt_project as dbt_project_mod

    project_dir, model, manifest = _mini_dbt_project(tmp_path)
    os.utime(model, (900, 900))
    os.utime(project_dir / "dbt_project.yml", (900, 900))
    os.utime(manifest, (1100, 1100))

    class FakePreparer:
        def using_dagster_dev(self):
            return False

        def prepare(self, _project):
            raise AssertionError("fresh manifests should be reused")

    project = SimpleNamespace(project_dir=project_dir, manifest_path=manifest)
    dbt_project_mod.prepare_dbt_project(project, preparer=FakePreparer())


def test_prepare_if_dev_skips_when_manifest_fresh(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from oddsfox_pipeline.orchestration.dbt_project import OddsfoxDbtProjectPreparer

    project_dir, model, manifest = _mini_dbt_project(tmp_path)
    os.utime(model, (900, 900))
    os.utime(project_dir / "dbt_project.yml", (900, 900))
    os.utime(manifest, (1100, 1100))
    prepared: list[str] = []
    project = SimpleNamespace(project_dir=project_dir, manifest_path=manifest)

    preparer = OddsfoxDbtProjectPreparer()
    monkeypatch.setattr(preparer, "using_dagster_dev", lambda: True)
    monkeypatch.setattr(
        preparer, "prepare", lambda proj: prepared.append(str(proj.manifest_path))
    )
    monkeypatch.delenv("ODDSFOX_DBT_FORCE_PREPARE", raising=False)
    preparer.prepare_if_dev(project)
    assert prepared == []


def test_prepare_if_dev_runs_when_model_newer(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from oddsfox_pipeline.orchestration.dbt_project import OddsfoxDbtProjectPreparer

    project_dir, model, manifest = _mini_dbt_project(tmp_path)
    os.utime(manifest, (1000, 1000))
    os.utime(project_dir / "dbt_project.yml", (900, 900))
    os.utime(model, (1100, 1100))
    prepared: list[str] = []
    project = SimpleNamespace(project_dir=project_dir, manifest_path=manifest)

    preparer = OddsfoxDbtProjectPreparer()
    monkeypatch.setattr(preparer, "using_dagster_dev", lambda: True)

    def _prepare(proj):
        prepared.append(str(proj.manifest_path))
        manifest.write_text('{"nodes": {}}\n', encoding="utf-8")

    monkeypatch.setattr(preparer, "prepare", _prepare)
    monkeypatch.delenv("ODDSFOX_DBT_FORCE_PREPARE", raising=False)
    preparer.prepare_if_dev(project)
    assert prepared == [str(manifest)]


def test_prepare_if_dev_runs_when_manifest_missing(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from oddsfox_pipeline.orchestration.dbt_project import OddsfoxDbtProjectPreparer

    project_dir, _model, manifest = _mini_dbt_project(tmp_path)
    manifest.unlink()
    prepared: list[str] = []
    project = SimpleNamespace(project_dir=project_dir, manifest_path=manifest)

    preparer = OddsfoxDbtProjectPreparer()
    monkeypatch.setattr(preparer, "using_dagster_dev", lambda: True)

    def _prepare(proj):
        prepared.append(str(proj.manifest_path))
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text('{"nodes": {}}\n', encoding="utf-8")

    monkeypatch.setattr(preparer, "prepare", _prepare)
    monkeypatch.delenv("ODDSFOX_DBT_FORCE_PREPARE", raising=False)
    preparer.prepare_if_dev(project)
    assert prepared == [str(manifest)]


def test_prepare_if_dev_force_env_overrides_fresh_manifest(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from oddsfox_pipeline.orchestration.dbt_project import OddsfoxDbtProjectPreparer

    project_dir, model, manifest = _mini_dbt_project(tmp_path)
    os.utime(model, (900, 900))
    os.utime(project_dir / "dbt_project.yml", (900, 900))
    os.utime(manifest, (1100, 1100))
    prepared: list[str] = []
    project = SimpleNamespace(project_dir=project_dir, manifest_path=manifest)

    preparer = OddsfoxDbtProjectPreparer()
    monkeypatch.setattr(preparer, "using_dagster_dev", lambda: True)

    def _prepare(proj):
        prepared.append(str(proj.manifest_path))

    monkeypatch.setattr(preparer, "prepare", _prepare)
    monkeypatch.setenv("ODDSFOX_DBT_FORCE_PREPARE", "1")
    preparer.prepare_if_dev(project)
    assert prepared == [str(manifest)]


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
        [
            "build",
            "--full-refresh",
            "--exclude",
            "tag:polygon_settlement tag:pmxt_order_book",
        ]
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

    assert captured_args == [["build", "--exclude", "tag:unrelated"]]


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

    assert captured_args == [
        [
            "build",
            "--exclude",
            "tag:polygon_settlement tag:pmxt_order_book",
        ]
    ]


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


def test_stream_dbt_build_runs_targeted_recovery_full_refresh(monkeypatch):
    from unittest.mock import MagicMock

    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            invocation = MagicMock()
            invocation.stream = lambda: iter(["event"])
            invocation.process = MagicMock(returncode=0)
            return invocation

    monkeypatch.setattr(
        dbt_build_mod,
        "polymarket_token_hourly_odds_incremental_recovery_needed",
        lambda: True,
    )
    cleared = {"count": 0}
    monkeypatch.setattr(
        dbt_build_mod,
        "clear_polymarket_token_hourly_odds_incremental_in_progress",
        lambda: cleared.__setitem__("count", cleared["count"] + 1),
    )

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                dbt_select="+polymarket_wc2026_market_hourly_odds",
            ),
        )
    )
    assert captured_args[0] == [
        "build",
        "--select",
        "int_polymarket_wc2026_token_hourly_odds",
        "--full-refresh",
    ]
    assert captured_args[1][0] == "build"
    assert cleared["count"] >= 2


def test_stream_dbt_build_skips_polymarket_recovery_for_kalshi_select(monkeypatch):
    from unittest.mock import MagicMock

    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            invocation = MagicMock()
            invocation.stream = lambda: iter(["event"])
            invocation.process = MagicMock(returncode=0)
            return invocation

    monkeypatch.setattr(
        dbt_build_mod,
        "polymarket_token_hourly_odds_incremental_recovery_needed",
        lambda: True,
    )
    marked = {"count": 0}
    monkeypatch.setattr(
        dbt_build_mod,
        "mark_polymarket_token_hourly_odds_incremental_in_progress",
        lambda: marked.__setitem__("count", marked["count"] + 1),
    )

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=MagicMock(),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(dbt_select="+tag:kalshi"),
        )
    )
    assert len(captured_args) == 1
    assert captured_args[0][0] == "build"
    assert marked["count"] == 0


def test_stream_dbt_build_recovers_soccer_incrementals_in_dependency_order(
    monkeypatch,
):
    captured_args: list[list[str]] = []
    marked: list[str] = []
    cleared: list[str] = []
    observed, dense = dbt_build_mod.POLYMARKET_SOCCER_INCREMENTAL_MODELS

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            invocation = MagicMock()
            invocation.stream = lambda: iter(["event"])
            invocation.process = MagicMock(returncode=0)
            return invocation

    monkeypatch.setattr(
        dbt_build_mod,
        "dbt_incremental_recovery_needed",
        lambda model: model == observed,
    )
    monkeypatch.setattr(
        dbt_build_mod, "mark_dbt_incremental_in_progress", marked.append
    )
    monkeypatch.setattr(
        dbt_build_mod, "clear_dbt_incremental_in_progress", cleared.append
    )

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=MagicMock(is_subset=False),
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(
                dbt_select="+polymarket_soccer_match_result_data_quality"
            ),
            scope_name="soccer",
        )
    )

    assert captured_args[0] == [
        "build",
        "--select",
        observed,
        "--full-refresh",
    ]
    assert marked == [observed, dense]
    assert cleared == [observed, observed, dense]


def test_polymarket_token_hourly_odds_incremental_in_scope_for_subset_keys():
    from unittest.mock import MagicMock

    from oddsfox_pipeline.orchestration.config import DbtBuildConfig
    from oddsfox_pipeline.orchestration.dbt_build import (
        _polymarket_token_hourly_odds_incremental_in_scope,
    )
    from oddsfox_pipeline.orchestration.shipped_scopes import POLYMARKET_WC2026_SCOPE
    from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (
        DBT_SOURCE_POLYMARKET_WC2026,
        dbt_model_asset_key_for_name,
    )

    selected = {
        dbt_model_asset_key_for_name(
            "int_polymarket_wc2026_token_hourly_odds",
            DBT_SOURCE_POLYMARKET_WC2026,
            layer="intermediate",
        ),
        dbt_model_asset_key_for_name(
            "polymarket_wc2026_market_hourly_odds",
            DBT_SOURCE_POLYMARKET_WC2026,
            layer="marts",
        ),
    }
    cfg = DbtBuildConfig(
        dbt_select=POLYMARKET_WC2026_SCOPE.dbt_select,
        dbt_exclude=POLYMARKET_WC2026_SCOPE.dbt_exclude,
    )
    assert _polymarket_token_hourly_odds_incremental_in_scope(
        config=cfg,
        context=MagicMock(is_subset=True, selected_asset_keys=selected),
        is_subset=True,
    )
    assert not _polymarket_token_hourly_odds_incremental_in_scope(
        config=cfg,
        context=MagicMock(is_subset=True, selected_asset_keys=set()),
        is_subset=True,
    )
    assert not _polymarket_token_hourly_odds_incremental_in_scope(
        config=cfg,
        context=MagicMock(
            is_subset=True,
            selected_asset_keys={"unrelated/model"},
        ),
        is_subset=True,
    )

    for selector in ("tag:polygon_settlement", "tag:pmxt_order_book"):
        assert not _polymarket_token_hourly_odds_incremental_in_scope(
            config=DbtBuildConfig(dbt_select=selector),
            context=MagicMock(is_subset=False),
            is_subset=False,
        )
    assert not _polymarket_token_hourly_odds_incremental_in_scope(
        config=DbtBuildConfig(dbt_select="unrelated_model"),
        context=MagicMock(is_subset=False),
        is_subset=False,
    )


def test_run_dbt_cli_to_completion_rejects_nonzero_exit():
    invocation = MagicMock()
    invocation.stream.return_value = iter(())
    invocation.process.returncode = 2
    dbt = MagicMock()
    dbt.cli.return_value = invocation

    with pytest.raises(RuntimeError, match="exit code 2"):
        dbt_build_mod._run_dbt_cli_to_completion(
            context=MagicMock(),
            dbt=dbt,
            build_args=["build"],
        )


def test_stream_dbt_build_subset_without_exclude_uses_plain_build():
    invocation = MagicMock()
    invocation.stream.return_value = iter(())
    invocation.process.returncode = 0
    dbt = MagicMock()
    dbt.cli.return_value = invocation

    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="oddsfox_dbt",
            context=MagicMock(is_subset=True, selected_asset_keys=set()),
            dbt=dbt,
            config=orch_config.DbtBuildConfig(dbt_exclude=None),
        )
    )

    assert dbt.cli.call_args.args[0] == ["build"]


def test_stream_dbt_build_persists_failure_metrics_on_recovery_error(monkeypatch):
    from unittest.mock import MagicMock

    saved: list[tuple[str, Exception]] = []

    class MockDbt:
        def cli(self, args, context=None):
            invocation = MagicMock()
            invocation.stream = lambda: iter(["event"])
            invocation.process = MagicMock(returncode=0)
            return invocation

    monkeypatch.setattr(
        dbt_build_mod,
        "polymarket_token_hourly_odds_incremental_recovery_needed",
        lambda: True,
    )
    monkeypatch.setattr(
        dbt_build_mod,
        "_polymarket_token_hourly_odds_incremental_in_scope",
        lambda **kwargs: True,
    )
    monkeypatch.setattr(
        dbt_build_mod,
        "_run_dbt_cli_to_completion",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("recovery boom")),
    )
    monkeypatch.setattr(
        dbt_build_mod,
        "save_asset_failure_metrics",
        lambda task, exc, **kwargs: saved.append((task, exc)),
    )

    with pytest.raises(RuntimeError, match="recovery boom"):
        list(
            dbt_build_mod.stream_dbt_build(
                asset_name="oddsfox_dbt",
                context=MagicMock(is_subset=False),
                dbt=MockDbt(),
                config=orch_config.DbtBuildConfig(
                    dbt_select="+polymarket_wc2026_market_hourly_odds",
                ),
            )
        )
    assert saved[0][0] == "dbt_build"
    assert isinstance(saved[0][1], RuntimeError)
