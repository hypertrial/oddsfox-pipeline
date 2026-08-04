"""Autouse guards for orchestration unit tests (avoid real sleeps and dbt stream blocking)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from oddsfox_pipeline.orchestration import dbt_build as dbt_build_mod
from oddsfox_pipeline.orchestration import pipeline_ops as pipeline_ops_mod
from oddsfox_pipeline.orchestration import polymarket_ops as polymarket_ops_mod
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail
from tests.unit.orchestration.orchestration_test_support import (
    _FakeClock,
    _ImmediateThread,
)
from tests.unit.storage.duckdb_storage_test_support import (
    initialize_isolated_duckdb,
    reload_settings_and_connection,
)


def _patch_progress_guardrail_module(monkeypatch, module, clock: _FakeClock) -> None:
    class _ClockedProgressGuardrail(ProgressGuardrail):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("clock", clock)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(module, "ProgressGuardrail", _ClockedProgressGuardrail)


def _stub_registry(**kwargs):
    cb = kwargs.get("progress_callback")
    if cb:
        cb("registry_probe", {"ok": True})
    return {"registry_rows_upserted": 0}


def _stub_sync_markets(**kwargs):
    cb = kwargs.get("progress_callback")
    if cb:
        cb("probe_markets", {"page": 1})
    return {"task": "sync_markets", "total_fetched": 0}


def _stub_sync_odds(**kwargs):
    cb = kwargs.get("progress_callback")
    if cb:
        cb("probe_odds", {"token": 1})
    return {"task": "sync_odds", "noop": True}


@pytest.fixture(autouse=True)
def orchestration_test_guards(request, monkeypatch, reset_connection_globals):
    """Keep orchestration unit tests off real wall-clock sleeps and blocking dbt polls."""
    del reset_connection_globals
    if request.node.get_closest_marker("facade") is not None:
        yield
        return

    clock = _FakeClock()
    for module in (
        pipeline_ops_mod,
        polymarket_ops_mod,
        dbt_build_mod,
    ):
        if hasattr(module, "ProgressGuardrail"):
            _patch_progress_guardrail_module(monkeypatch, module, clock)

    import oddsfox_pipeline.orchestration.assets as assets_mod

    if hasattr(assets_mod, "ProgressGuardrail"):
        _patch_progress_guardrail_module(monkeypatch, assets_mod, clock)

    monkeypatch.setattr(
        polymarket_ops_mod, "sync_market_scope_registry", _stub_registry
    )
    monkeypatch.setattr(polymarket_ops_mod, "sync_markets", _stub_sync_markets)
    monkeypatch.setattr(polymarket_ops_mod, "sync_odds", _stub_sync_odds)
    monkeypatch.setattr(
        polymarket_ops_mod,
        "enrich_market_metadata",
        lambda **kwargs: {"task": "enrich_market_metadata", "skipped": True},
    )
    monkeypatch.setattr(
        pipeline_ops_mod,
        "reconcile_odds_ledger",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    monkeypatch.setattr(dbt_build_mod, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        dbt_build_mod,
        "mark_polymarket_token_hourly_odds_incremental_in_progress",
        lambda: None,
    )
    monkeypatch.setattr(
        dbt_build_mod,
        "clear_polymarket_token_hourly_odds_incremental_in_progress",
        lambda: None,
    )
    monkeypatch.setattr(
        dbt_build_mod,
        "polymarket_token_hourly_odds_incremental_recovery_needed",
        lambda: False,
    )
    monkeypatch.setattr(dbt_build_mod, "save_sync_run_metrics", lambda *_a, **_k: None)
    monkeypatch.setattr(
        dbt_build_mod, "save_asset_failure_metrics", lambda *_a, **_k: None
    )

    with patch("time.sleep", lambda *_a, **_k: None):
        yield


@pytest.fixture
def orchestration_isolated_settings(monkeypatch, tmp_path):
    """Opt-in settings/connection reload for tests that mutate DUCKDB_* env."""
    db_path = tmp_path / "orchestration.duckdb"
    connection = reload_settings_and_connection(monkeypatch, db_path)
    yield connection
    connection.reset_duckdb_connection_state()


@pytest.fixture
def orchestration_duckdb(monkeypatch, tmp_path):
    """Opt-in real DuckDB bootstrap for orchestration tests that touch storage."""
    db_path = tmp_path / "orchestration.duckdb"
    connection = initialize_isolated_duckdb(monkeypatch, db_path)
    yield connection
    connection.reset_duckdb_connection_state()
