from __future__ import annotations

from oddsfox_pipeline.orchestration.failure_metrics import (
    build_failure_metrics,
    save_asset_failure_metrics,
)


def test_build_failure_metrics_includes_summary_and_extra() -> None:
    class _Err(Exception):
        summary = {"status": "fetch_failed", "tokens": 3}

    payload = build_failure_metrics(
        _Err("boom"),
        extra={"asset": "sync_odds"},
    )
    assert payload["status"] == "failed"
    assert payload["failure_status"] == "fetch_failed"
    assert payload["tokens"] == 3
    assert payload["error_type"] == "_Err"
    assert payload["error_message"] == "boom"
    assert payload["asset"] == "sync_odds"


def test_save_asset_failure_metrics_delegates_to_save_fn() -> None:
    captured: list[tuple[str, dict]] = []

    def _save(task: str, metrics: dict, **kwargs) -> None:
        captured.append((task, metrics))

    save_asset_failure_metrics(
        "event_catalog",
        RuntimeError("gamma down"),
        scope_name="wc2026",
        save_fn=_save,
    )
    assert captured[0][0] == "event_catalog"
    assert captured[0][1]["status"] == "failed"
    assert captured[0][1]["error_type"] == "RuntimeError"


def test_save_asset_failure_metrics_swallows_persist_errors() -> None:
    def _save(task: str, metrics: dict, **kwargs) -> None:
        raise RuntimeError("duckdb locked")

    save_asset_failure_metrics(
        "event_catalog",
        RuntimeError("gamma down"),
        save_fn=_save,
    )
