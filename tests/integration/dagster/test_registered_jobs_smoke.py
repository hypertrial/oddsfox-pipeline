from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import ResourceDefinition

from oddsfox.orchestration import assets_polymarket as assets_mod
from oddsfox.orchestration.definitions import defs


def _registered_job_names() -> list[str]:
    return sorted(
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    )


@pytest.fixture
def patched_dagster_runtime(monkeypatch, tmp_path):
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
    def connection():
        yield conn

    def stream_dbt_build(**_kwargs):
        if False:
            yield None

    monkeypatch.setattr(assets_mod, "get_polymarket_dlt_pipeline", lambda: pipeline)
    monkeypatch.setattr(assets_mod, "collect_raw_markets", lambda: [])
    monkeypatch.setattr(
        assets_mod, "normalize_market_payloads_for_dlt", lambda _rows: []
    )
    monkeypatch.setattr(assets_mod, "get_connection", connection)
    monkeypatch.setattr(assets_mod, "ensure_polymarket_indexes", lambda _conn: None)
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})
    monkeypatch.setattr(assets_mod, "snapshot_dbt_models", lambda: {})
    monkeypatch.setattr(assets_mod, "delta_dbt_models", lambda _pre, _post: {})
    monkeypatch.setattr(assets_mod, "format_raw_snapshot_log", lambda _snapshot: "")
    monkeypatch.setattr(assets_mod, "format_dbt_snapshot_log", lambda _snapshot: "")
    monkeypatch.setattr(assets_mod, "get_sync_run_metrics", lambda _task: None)
    monkeypatch.setattr(assets_mod.ops, "stream_dbt_build", stream_dbt_build)
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
        "sync_wc2026_registry",
        lambda **_kwargs: {"registry_rows_upserted": 0},
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "backfill_market_metadata",
        lambda **_kwargs: {"task": "backfill_market_metadata", "skipped": True},
    )
    monkeypatch.setattr(assets_mod.ops, "delete_orphan_market_tokens", lambda: 0)
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_odds",
        lambda **_kwargs: {"planning": {}, "planning_context": {}, "totals": {}},
    )

    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter([])
    return {
        "dbt": ResourceDefinition.hardcoded_resource(MagicMock()),
        "dlt": ResourceDefinition.hardcoded_resource(fake_dlt),
    }


@pytest.mark.parametrize("job_name", _registered_job_names())
def test_registered_dagster_job_executes(job_name, patched_dagster_runtime):
    result = defs.resolve_job_def(job_name).execute_in_process(
        resources=patched_dagster_runtime,
    )

    assert result.success is True
