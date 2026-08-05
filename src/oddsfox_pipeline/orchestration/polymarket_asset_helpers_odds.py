"""Polymarket odds-sync asset helpers."""

from __future__ import annotations

from typing import Any, Callable

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.config import OddsSyncConfig
from oddsfox_pipeline.orchestration.failure_metrics import save_asset_failure_metrics
from oddsfox_pipeline.orchestration.raw_snapshot_helpers import _run_with_raw_snapshot
from oddsfox_pipeline.orchestration.transient_retry import raise_retry_if_transient


def _build_odds_sync_kwargs(
    config: OddsSyncConfig,
    progress_callback: Callable[[str, dict[str, Any]], None],
    *,
    market_scope: str = SCOPE_WC2026,
    plan_iterator_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    sync_kwargs: dict[str, Any] = {
        "max_workers": config.workers,
        "batch_size": config.batch_size,
        "fidelity": config.fidelity,
        "requests_per_second": config.requests_per_second,
        "auto_tune_rps": config.auto_tune_rps,
        "auto_tune_max_rps": config.auto_tune_max_rps,
        "force": config.force,
        "clob_cutoff_date": config.clob_cutoff,
        "skip_recent_minutes": config.skip_recent_minutes,
        "overlap_minutes": config.overlap_minutes,
        "window_hours": config.window_hours,
        "rebuild_history": config.rebuild_history,
        "reconcile_ledger": config.reconcile_ledger,
        "short_range_first": config.short_range_first,
        "market_scope": market_scope,
        "ended_market_grace_days": config.ended_market_grace_days,
        "min_volume": config.min_volume,
        "max_volume": config.max_volume,
        "history_backfill_days": config.history_backfill_days,
        "empty_token_skip_runs": config.empty_skip_runs,
        "batch_group_size": config.batch_group_size,
        "routine_interval_hours": config.routine_interval_hours,
        "empty_retry_base_hours": config.empty_retry_base_hours,
        "empty_retry_max_hours": config.empty_retry_max_hours,
        "error_retry_minutes": config.error_retry_minutes,
        "transient_retries": config.transient_retries,
        "transient_backoff_seconds": config.transient_backoff_seconds,
        "market_page_size": config.market_page_size,
        "progress_callback": progress_callback,
        "progress_log_interval_tokens": config.progress_log_interval_tokens,
        "progress_log_interval_seconds": config.progress_log_interval_seconds,
        "no_progress_soft_timeout_seconds": config.no_progress_soft_timeout_seconds,
        "no_progress_hard_timeout_seconds": config.no_progress_hard_timeout_seconds,
        "progress_poll_seconds": config.progress_poll_seconds,
        "progress_logger": None,
    }
    if plan_iterator_factory is not None:
        sync_kwargs["plan_iterator_factory"] = plan_iterator_factory
    return sync_kwargs


def _odds_sync_metadata(
    config: OddsSyncConfig,
    run_summary: dict[str, Any],
    raw_metadata: dict[str, MetadataValue],
) -> dict[str, MetadataValue]:
    metadata = {
        "workers": MetadataValue.int(config.workers),
        "force": MetadataValue.bool(config.force),
        "fidelity": MetadataValue.int(config.fidelity),
        "history_backfill_days": MetadataValue.int(config.history_backfill_days),
        "planning": MetadataValue.json(run_summary.get("planning", {})),
        "planning_context": MetadataValue.json(run_summary.get("planning_context", {})),
        "totals": MetadataValue.json(run_summary.get("totals", {})),
        **raw_metadata,
    }
    if config.min_volume is not None:
        metadata["min_volume"] = MetadataValue.float(config.min_volume)
    if config.max_volume is not None:
        metadata["max_volume"] = MetadataValue.float(config.max_volume)
    return metadata


def _materialize_odds_sync(
    context: AssetExecutionContext,
    config: OddsSyncConfig,
    *,
    market_scope: str = SCOPE_WC2026,
    plan_iterator_factory: Callable[..., Any] | None = None,
    sync_odds_fn: Callable[..., dict[str, Any]] = ops.sync_odds,
    run_with_raw_snapshot_fn: Callable[
        ...,
        tuple[
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            dict[str, MetadataValue],
        ],
    ] = _run_with_raw_snapshot,
) -> MaterializeResult:
    def _odds_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)

    sync_kwargs = _build_odds_sync_kwargs(
        config,
        _odds_progress,
        market_scope=market_scope,
        plan_iterator_factory=plan_iterator_factory,
    )
    sync_kwargs["progress_logger"] = context.log
    try:
        run_summary, _, _, _, raw_metadata = run_with_raw_snapshot_fn(
            config.raw_snapshot_level,
            lambda _pre: sync_odds_fn(**sync_kwargs),
        )
    except Exception as exc:
        save_asset_failure_metrics(
            "sync_odds",
            exc,
            scope_name=market_scope,
        )
        raise_retry_if_transient(exc)
        raise
    metadata = _odds_sync_metadata(config, run_summary, raw_metadata)
    return MaterializeResult(metadata=metadata)


__all__ = [
    "_build_odds_sync_kwargs",
    "_materialize_odds_sync",
    "_odds_sync_metadata",
]
