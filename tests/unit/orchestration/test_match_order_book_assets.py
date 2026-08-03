from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    MatchOrderBookSyncError,
)
from oddsfox_pipeline.orchestration import assets_match_order_book as assets_mod
from oddsfox_pipeline.orchestration.config import MatchOrderBookBackfillConfig
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail


def _connection(value="connection") -> MagicMock:
    connection = MagicMock()
    connection.__enter__.return_value = value
    return connection


def test_match_order_book_sync_bridge_forwards_bounded_config(monkeypatch):
    sync = MagicMock(return_value={"status": "published"})
    monkeypatch.setattr(assets_mod, "sync_match_order_book_history", sync)
    config = MatchOrderBookBackfillConfig(
        requests_per_minute=40,
        monthly_credit_budget=12_000,
        transient_retries=2,
        transient_backoff_seconds=0.5,
        force=True,
        manifest_path="/tmp/target.yml",
    )
    progress = MagicMock()

    result = assets_mod._sync_match_order_book(
        "connection",
        config,
        lease_owner="run-1",
        progress_callback=progress,
    )

    assert result == {"status": "published"}
    sync.assert_called_once_with(
        "connection",
        requests_per_minute=40,
        monthly_credit_budget=12_000,
        transient_retries=2,
        transient_backoff_seconds=0.5,
        force=True,
        lease_owner="run-1",
        progress_callback=progress,
        manifest_path=Path("/tmp/target.yml"),
    )
    sync.reset_mock()

    assets_mod._sync_match_order_book(
        "connection",
        config.model_copy(update={"manifest_path": None}),
        lease_owner="run-1",
        progress_callback=progress,
    )

    assert "manifest_path" not in sync.call_args.kwargs


def test_match_order_book_asset_emits_progress_and_summary(monkeypatch):
    connection = _connection()
    monkeypatch.setattr(assets_mod, "get_connection", lambda: connection)

    def sync(_conn, _config, *, lease_owner, progress_callback):
        assert lease_owner == "run-1"
        progress_callback("loaded", {"snapshots": 2})
        return {"status": "published", "snapshot_count": 2}

    monkeypatch.setattr(assets_mod, "_sync_match_order_book", sync)
    context = MagicMock()
    context.run.run_id = "run-1"

    result = assets_mod.polymarket_wc2026_raw_match_order_book_snapshots.op.compute_fn.decorated_fn(
        context,
        MatchOrderBookBackfillConfig(),
    )

    assert result.metadata["status"] == "published"
    assert result.metadata["snapshot_count"] == 2


def test_match_order_book_asset_attaches_allowlisted_failure_metadata(monkeypatch):
    connection = _connection()
    monkeypatch.setattr(assets_mod, "get_connection", lambda: connection)
    failure = MatchOrderBookSyncError(
        "paused",
        {
            "status": "paused",
            "remaining_window_count": 2,
            "ratio": 0.5,
            "optional": None,
            "unsafe": ["not", "metadata"],
        },
    )
    monkeypatch.setattr(
        assets_mod,
        "_sync_match_order_book",
        MagicMock(side_effect=failure),
    )
    context = MagicMock()
    context.run.run_id = "run-1"

    with pytest.raises(MatchOrderBookSyncError, match="paused"):
        assets_mod.polymarket_wc2026_raw_match_order_book_snapshots.op.compute_fn.decorated_fn(
            context,
            MatchOrderBookBackfillConfig(),
        )

    context.add_output_metadata.assert_called_once_with(
        {
            "status": "paused",
            "remaining_window_count": 2,
            "ratio": 0.5,
        }
    )


def test_match_order_book_progress_records_before_timeout_check():
    class _Logger:
        def info(self, *_args, **_kwargs):
            return None

        def warning(self, *_args, **_kwargs):
            return None

        def error(self, *_args, **_kwargs):
            return None

    clock = {"t": 0.0}
    guardrail = ProgressGuardrail(
        asset="polymarket_wc2026_raw_match_order_book_snapshots",
        logger=_Logger(),
        progress_log_interval_seconds=60,
        no_progress_soft_timeout_seconds=5,
        no_progress_hard_timeout_seconds=10,
        clock=lambda: clock["t"],
    )

    def progress(phase: str, diagnostics: dict[str, object]) -> None:
        guardrail.record_progress(
            work_increment=1, phase=phase, diagnostics=diagnostics
        )
        guardrail.check(phase=phase, diagnostics=diagnostics)

    clock["t"] = 11.0
    progress("loaded", {"snapshots": 1})
    assert guardrail.snapshot()["work_completed"] == 1
