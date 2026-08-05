"""Shared Polymarket asset helpers and re-exports for split subject modules."""

from __future__ import annotations

from typing import Any, Callable

from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.raw_snapshot_helpers import (
    _raw_snapshot_metadata,
    _run_with_raw_snapshot,
)


def _run_with_guardrail_thread(
    guardrail: Any,
    phase_name: str,
    run_fn: Callable[[], dict[str, Any]],
    *,
    poll_seconds: float,
    thread_factory: Callable[..., Any] = ops.Thread,
) -> dict[str, Any]:
    result: dict[str, Any] | None = None
    error: Exception | None = None

    def _target() -> None:
        nonlocal result, error
        try:
            result = run_fn()
        except Exception as exc:
            error = exc

    worker = thread_factory(target=_target, daemon=True)
    worker.start()
    try:
        while worker.is_alive():
            worker.join(timeout=max(1, poll_seconds))
            if worker.is_alive():
                guardrail.check(
                    phase=phase_name,
                    diagnostics={"worker_alive": True},
                )
    except BaseException:
        if worker.is_alive():
            worker.join()
        raise
    if error is not None:
        raise error
    guardrail.record_progress(
        work_increment=0,
        phase=f"{phase_name}_complete",
        diagnostics={"worker_alive": False},
        force_log=True,
    )
    return result or {}


from oddsfox_pipeline.orchestration.polymarket_asset_helpers_markets import (  # noqa: E402
    _materialize_raw_markets_snapshot,
    _run_raw_markets,
)
from oddsfox_pipeline.orchestration.polymarket_asset_helpers_odds import (  # noqa: E402
    _build_odds_sync_kwargs,
    _materialize_odds_sync,
    _odds_sync_metadata,
)
from oddsfox_pipeline.orchestration.polymarket_asset_helpers_registry import (  # noqa: E402
    _materialize_event_catalog,
    _materialize_market_scope_registry,
    _materialize_metadata_enrichment,
)

__all__ = [
    "_build_odds_sync_kwargs",
    "_materialize_event_catalog",
    "_materialize_market_scope_registry",
    "_materialize_metadata_enrichment",
    "_materialize_odds_sync",
    "_materialize_raw_markets_snapshot",
    "_odds_sync_metadata",
    "_raw_snapshot_metadata",
    "_run_raw_markets",
    "_run_with_guardrail_thread",
    "_run_with_raw_snapshot",
]
