from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import MaterializeResult, ResourceDefinition

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.orchestration import (
    assets_international_results as results_assets_mod,
)
from oddsfox_pipeline.orchestration import assets_kalshi_wc2026 as kalshi_assets_mod
from oddsfox_pipeline.orchestration import (
    assets_openfootball as openfootball_assets_mod,
)
from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
from oddsfox_pipeline.orchestration import (
    assets_polymarket_us_midterms_2026 as midterms_assets_mod,
)
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.scope_registry import SCOPE_STEPS, iter_scope_specs

_NON_SCOPE_JOB_NAMES = {
    "international_results_historical_ingest",
    "international_results_wc2026_match_results_ingest",
    "wc2026_knockout_match_odds_full_pipeline",
    "polymarket_wc2026_match_minute_odds_backfill",
}


def _expected_public_job_names() -> set[str]:
    scoped_jobs = {
        spec.job_for_step(step) for spec in iter_scope_specs() for step in SCOPE_STEPS
    }
    return scoped_jobs | _NON_SCOPE_JOB_NAMES


def _registered_job_names() -> list[str]:
    return sorted(
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    )


@pytest.fixture
def patched_dagster_runtime(monkeypatch, tmp_path):
    connection.reset_duckdb_connection_state()
    db_path = tmp_path / "registered_jobs.duckdb"
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

    pipeline = MagicMock(has_pending_data=False)
    conn = MagicMock()

    @contextmanager
    def mock_connection():
        yield conn

    def stream_dbt_build(**_kwargs):
        if False:
            yield None

    for module in (assets_mod, midterms_assets_mod, kalshi_assets_mod):
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
                module,
                "save_market_tokens_batch",
                lambda *_args, **_kwargs: None,
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

    monkeypatch.setattr(assets_mod, "snapshot_dbt_models", lambda: {})
    monkeypatch.setattr(assets_mod, "delta_dbt_models", lambda _pre, _post: {})
    monkeypatch.setattr(assets_mod, "format_raw_snapshot_log", lambda _snapshot: "")
    monkeypatch.setattr(assets_mod, "format_dbt_snapshot_log", lambda _snapshot: "")
    monkeypatch.setattr(assets_mod.ops, "stream_dbt_build", stream_dbt_build)
    monkeypatch.setattr(
        results_assets_mod,
        "sync_wc2026_match_results",
        lambda: {"rows": 0, "completed_rows": 0, "scheduled_rows": 0},
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
        "sync_knockout_fixtures",
        lambda: {"rows": 0, "completed_rows": 0, "scheduled_rows": 0},
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
        "backfill_market_metadata",
        lambda **_kwargs: {"task": "backfill_market_metadata", "skipped": True},
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

    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter([])
    try:
        yield {
            "dbt": ResourceDefinition.hardcoded_resource(MagicMock()),
            "dlt": ResourceDefinition.hardcoded_resource(fake_dlt),
        }
    finally:
        connection.reset_duckdb_connection_state()


def test_registered_dagster_jobs_match_shipped_scope_inventory():
    assert set(_registered_job_names()) == _expected_public_job_names()


@pytest.mark.parametrize("job_name", _registered_job_names())
def test_registered_dagster_job_executes(job_name, patched_dagster_runtime):
    result = defs.resolve_job_def(job_name).execute_in_process(
        resources=patched_dagster_runtime,
    )

    assert result.success is True
