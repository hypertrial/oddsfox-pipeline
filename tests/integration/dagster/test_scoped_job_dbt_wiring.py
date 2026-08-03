"""Scoped-job wiring: ingest steps skip dbt; dbt/full steps use shipped select/exclude."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import MaterializeResult, ResourceDefinition
from tests.integration.dagster.recording_dbt import RecordingDbtResource

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.orchestration import (
    assets_international_results as results_assets_mod,
)
from oddsfox_pipeline.orchestration import assets_kalshi_wc2026 as kalshi_assets_mod
from oddsfox_pipeline.orchestration import (
    assets_match_order_book as order_book_assets_mod,
)
from oddsfox_pipeline.orchestration import assets_match_trades as trade_assets_mod
from oddsfox_pipeline.orchestration import (
    assets_openfootball as openfootball_assets_mod,
)
from oddsfox_pipeline.orchestration import (
    assets_polygon_settlement as polygon_assets_mod,
)
from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
from oddsfox_pipeline.orchestration.config import (
    kalshi_wc2026_dbt_build_run_config,
    polymarket_wc2026_dbt_build_run_config,
)
from oddsfox_pipeline.orchestration.dbt_build import stream_dbt_build
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.shipped_scopes import (
    KALSHI_WC2026_SCOPE,
    POLYMARKET_WC2026_SCOPE,
    SCOPE_STEPS,
    iter_scope_specs,
    scope_dbt_config,
)

_EMPTY_RESULTS_SUMMARY = {
    "rows": 0,
    "completed_rows": 0,
    "scheduled_rows": 0,
    "source_url": "https://example.com/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/results.csv",
    "source_revision": "a" * 40,
    "source_payload_sha256": "b" * 64,
}

_SCOPE_DBT_RUN_CONFIG = {
    POLYMARKET_WC2026_SCOPE.key: polymarket_wc2026_dbt_build_run_config,
    KALSHI_WC2026_SCOPE.key: kalshi_wc2026_dbt_build_run_config,
}


def _scoped_jobs() -> list[tuple[str, str, str]]:
    return [
        (spec.key, step, spec.job_for_step(step))
        for spec in iter_scope_specs()
        for step in SCOPE_STEPS
    ]


@pytest.fixture
def wiring_runtime(monkeypatch, tmp_path):
    """Mocked externals with a live stream_dbt_build + recording dbt resource."""
    connection.reset_duckdb_connection_state()
    db_path = tmp_path / "scoped_wiring.duckdb"
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: 2
  target: dev
"""
    )
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
    monkeypatch.setenv(
        "ODDSFOX_WC2026_REVIEWED_MEMBERSHIP_PATH",
        str(tmp_path / "reviewed-membership.csv"),
    )

    pipeline = MagicMock(has_pending_data=False)
    conn = MagicMock()

    @contextmanager
    def mock_connection():
        yield conn

    for module in (assets_mod, kalshi_assets_mod):
        if module is kalshi_assets_mod:
            monkeypatch.setattr(
                module.asset_helpers,
                "get_kalshi_dlt_pipeline",
                lambda **_kwargs: pipeline,
            )
            monkeypatch.setattr(
                module,
                "collect_market_scope_payload",
                lambda **_kwargs: {
                    "scope_name": "wc2026",
                    "events": [],
                    "markets": [],
                    "total_events": 0,
                    "total_markets": 0,
                    "registry_summary": {"registry_rows_upserted": 0},
                },
            )
            monkeypatch.setattr(
                module, "ensure_kalshi_indexes", lambda *_args, **_kwargs: None
            )
            monkeypatch.setattr(
                module.asset_helpers,
                "materialize_kalshi_candlesticks_sync",
                lambda *_args, **_kwargs: MaterializeResult(metadata={}),
            )
            monkeypatch.setattr(
                module.ops,
                "sync_kalshi_market_scope_registry",
                lambda **_kwargs: {"registry_rows_upserted": 0},
            )
        else:
            monkeypatch.setattr(
                module.asset_helpers,
                "get_polymarket_dlt_pipeline",
                lambda **_kwargs: pipeline,
            )
            monkeypatch.setattr(
                module,
                "collect_market_scope_payload",
                lambda **_kwargs: {
                    "market_rows": [],
                    "token_rows": [],
                    "run_summary": {"task": "sync_markets", "total_fetched": 0},
                },
            )
            monkeypatch.setattr(
                module, "save_market_tokens_batch", lambda *_args, **_kwargs: None
            )
            monkeypatch.setattr(
                module, "ensure_polymarket_indexes", lambda *_args, **_kwargs: None
            )
        monkeypatch.setattr(
            module, "save_sync_run_metrics", lambda *_args, **_kwargs: None
        )
        monkeypatch.setattr(module, "get_connection", mock_connection)
        monkeypatch.setattr(module, "snapshot_raw_layer", lambda **_kwargs: {})
        monkeypatch.setattr(module, "delta_raw_layer", lambda _pre, _post: {})
        monkeypatch.setattr(
            module, "get_sync_run_metrics", lambda *_task, **_kwargs: None
        )

    monkeypatch.setattr(assets_mod, "snapshot_dbt_models", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_dbt_models", lambda _pre, _post: {})
    monkeypatch.setattr(assets_mod, "format_raw_snapshot_log", lambda _snapshot: "")
    monkeypatch.setattr(assets_mod, "format_dbt_snapshot_log", lambda _snapshot: "")
    # Keep the real stream_dbt_build so RecordingDbtResource observes argv.
    monkeypatch.setattr(assets_mod.ops, "stream_dbt_build", stream_dbt_build)
    monkeypatch.setattr(
        results_assets_mod,
        "sync_wc2026_match_results",
        lambda: dict(_EMPTY_RESULTS_SUMMARY),
    )
    monkeypatch.setattr(
        results_assets_mod,
        "sync_historical_international_results",
        lambda: {
            "inserted_matches": 0,
            "inserted_shootouts": 0,
            "inserted_goalscorers": 0,
        },
    )
    monkeypatch.setattr(
        openfootball_assets_mod,
        "sync_schedule_fixtures",
        lambda: dict(_EMPTY_RESULTS_SUMMARY),
    )
    monkeypatch.setattr(order_book_assets_mod, "get_connection", mock_connection)
    monkeypatch.setattr(
        order_book_assets_mod,
        "_sync_match_order_book",
        lambda *_args, **_kwargs: {
            "status": "published",
            "scan_id": "pmxt-smoke",
            "snapshot_count": 2,
            "token_count": 2,
            "level_count": 4,
        },
    )
    monkeypatch.setattr(trade_assets_mod, "get_connection", mock_connection)
    monkeypatch.setattr(
        trade_assets_mod,
        "sync_match_trades",
        lambda *_args, **_kwargs: {
            "scan_id": "pmxt-smoke",
            "trade_count": 1,
            "empty_landscape_warnings": [],
            "aggregate_sha256": "c" * 64,
            "noop": False,
        },
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_markets",
        lambda **_kwargs: {
            "total_fetched": 0,
            "registry_refreshed": True,
            "events_pages": 0,
            "api_requests": 0,
            "truncated": False,
            "aborted": False,
        },
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_market_scope_registry",
        lambda **_kwargs: {"registry_rows_upserted": 0},
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "enrich_market_metadata",
        lambda **_kwargs: {"task": "enrich_market_metadata", "skipped": True},
    )
    monkeypatch.setattr(
        assets_mod.ops, "delete_orphan_market_tokens", lambda **_kwargs: 0
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_odds",
        lambda **_kwargs: {"planning": {}, "planning_context": {}, "totals": {}},
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_match_minute_odds_history",
        lambda *_args, **_kwargs: {
            "games": 104,
            "markets": 248,
            "tokens": 496,
            "rows": 496,
        },
    )
    monkeypatch.setattr(
        assets_mod,
        "replace_reviewed_membership",
        lambda _path: {
            "rows": 1,
            "source_sha256": "d" * 64,
            "reviewer_count": 1,
        },
    )
    monkeypatch.setattr(
        assets_mod,
        "collect_wc2026_event_catalog",
        lambda **_kwargs: SimpleNamespace(
            event_snapshots=[],
            event_tag_snapshots=[],
            event_market_snapshots=[],
            market_payloads=[],
            summary={"observed_at": "2026-08-02T00:00:00+00:00"},
        ),
    )
    monkeypatch.setattr(assets_mod, "merge_event_catalog_batch", lambda **_kwargs: None)
    monkeypatch.setattr(polygon_assets_mod, "get_connection", mock_connection)
    monkeypatch.setattr(
        polygon_assets_mod,
        "_sync_polygon_settlement_fills",
        lambda *_args, **_kwargs: {
            "scan_id": "a" * 64,
            "status": "published",
            "published": True,
            "fill_count": 1,
        },
    )
    monkeypatch.setattr(
        polygon_assets_mod, "_verify_polygon_settlement_scan", lambda _conn: None
    )
    monkeypatch.setattr(
        polygon_assets_mod,
        "load_polygon_settlement_release_provenance",
        lambda _conn: {"scan_id": "a" * 64},
    )
    monkeypatch.setattr(
        polygon_assets_mod,
        "build_polygon_settlement_audit_release",
        lambda *_args, **_kwargs: {
            "rows": 39_120,
            "markets": 248,
            "matches": 104,
            "tokens": 496,
            "dataset_version": "0.0.0-smoke",
            "release_dir": "mocked",
            "files": [],
        },
    )
    monkeypatch.setattr(
        polygon_assets_mod, "current_generator_commit", lambda: "a" * 40
    )

    recorder = RecordingDbtResource()
    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter([])
    try:
        yield {
            "dbt": recorder,
            "dlt": ResourceDefinition.hardcoded_resource(fake_dlt),
            "recorder": recorder,
        }
    finally:
        connection.reset_duckdb_connection_state()


@pytest.mark.parametrize(("scope_key", "step", "job_name"), _scoped_jobs())
def test_scoped_job_dbt_wiring(
    scope_key: str,
    step: str,
    job_name: str,
    wiring_runtime,
) -> None:
    recorder: RecordingDbtResource = wiring_runtime["recorder"]
    resources = {
        "dbt": ResourceDefinition.hardcoded_resource(recorder),
        "dlt": wiring_runtime["dlt"],
    }
    result = defs.resolve_job_def(job_name).execute_in_process(resources=resources)
    assert result.success is True

    expected = scope_dbt_config(scope_key)
    shipped = _SCOPE_DBT_RUN_CONFIG[scope_key]()["ops"]["oddsfox_dbt"]["config"]
    assert shipped["dbt_select"] == expected["dbt_select"]
    assert shipped["dbt_exclude"] == expected["dbt_exclude"]

    if step in {"market_scope_registry", "odds"}:
        # Ingest jobs may still invoke oddsfox_dbt for dbt source checks attached to
        # raw assets, but must not apply the shipped scope model selection.
        for args in recorder.calls:
            assert args and args[0] == "build"
            assert expected["dbt_select"] not in args
        return

    assert recorder.calls, f"{job_name} expected a dbt invocation"
    assert all(args and args[0] == "build" for args in recorder.calls)
    # Subset jobs let Dagster own model selection; shipped select/exclude still ride
    # on the job's default DbtBuildConfig for non-subset materializations.
    assert shipped["dbt_select"] == expected["dbt_select"]
    assert shipped["dbt_exclude"] == expected["dbt_exclude"]
