from __future__ import annotations

from typing import Any, Callable

import dlt
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from oddsfox.orchestration import polymarket_ops as ops
from oddsfox.orchestration.config import OddsSyncConfig
from oddsfox.storage.duckdb.connection import active_duckdb_path
from oddsfox.storage.duckdb.observability import (
    delta_raw_layer,
    snapshot_raw_layer,
)


def _raw_snapshot_metadata(
    pre: dict[str, Any],
    post: dict[str, Any],
    delta: dict[str, Any],
    *,
    run_summary: dict[str, Any] | None = None,
) -> dict[str, MetadataValue]:
    metadata = {
        "duckdb_raw_pre": MetadataValue.json(pre),
        "duckdb_raw_post": MetadataValue.json(post),
        "duckdb_raw_delta": MetadataValue.json(delta),
    }
    if run_summary is not None:
        metadata["run_summary"] = MetadataValue.json(run_summary)
    return metadata


def _run_with_raw_snapshot(
    raw_snapshot_level: str,
    run_fn: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    snapshot_raw_layer_fn: Callable[..., dict[str, Any]] = snapshot_raw_layer,
    delta_raw_layer_fn: Callable[
        [dict[str, Any], dict[str, Any]], dict[str, Any]
    ] = delta_raw_layer,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, MetadataValue],
]:
    pre = snapshot_raw_layer_fn(level=raw_snapshot_level)
    run_summary = run_fn(pre)
    post = snapshot_raw_layer_fn(level=raw_snapshot_level)
    delta = delta_raw_layer_fn(pre, post)
    return (
        run_summary,
        pre,
        post,
        delta,
        _raw_snapshot_metadata(
            pre,
            post,
            delta,
            run_summary=run_summary,
        ),
    )


def _build_odds_sync_kwargs(
    config: OddsSyncConfig,
    progress_callback: Callable[[str, dict[str, Any]], None],
    *,
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
        "rebuild_minutely": config.rebuild_minutely,
        "reconcile_ledger": config.reconcile_ledger,
        "short_range_first": config.short_range_first,
        "market_scope": config.scope_names,
        "ended_market_grace_days": config.ended_market_grace_days,
        "min_volume": config.min_volume,
        "max_volume": config.max_volume,
        "minutely_backfill_days": config.minutely_backfill_days,
        "empty_token_skip_runs": config.empty_skip_runs,
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
        "minutely_backfill_days": MetadataValue.int(config.minutely_backfill_days),
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
    while worker.is_alive():
        worker.join(timeout=max(1, poll_seconds))
        if worker.is_alive():
            guardrail.check(
                phase=phase_name,
                diagnostics={"worker_alive": True},
            )
    if error is not None:
        raise error
    guardrail.record_progress(
        work_increment=0,
        phase=f"{phase_name}_complete",
        diagnostics={"worker_alive": False},
        force_log=True,
    )
    return result or {}


def _materialize_odds_sync(
    context: AssetExecutionContext,
    config: OddsSyncConfig,
    *,
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
        plan_iterator_factory=plan_iterator_factory,
    )
    run_summary, _, _, _, raw_metadata = run_with_raw_snapshot_fn(
        config.raw_snapshot_level,
        lambda _pre: sync_odds_fn(**sync_kwargs),
    )
    metadata = _odds_sync_metadata(config, run_summary, raw_metadata)
    return MaterializeResult(metadata=metadata)


_DLT_PIPELINE_BY_PATH: dict[str, dlt.Pipeline] = {}


def get_polymarket_dlt_pipeline(
    *,
    active_duckdb_path_fn: Callable[[], Any] = active_duckdb_path,
    dlt_module: Any = dlt,
) -> dlt.Pipeline:
    db_path = str(active_duckdb_path_fn())
    cached = _DLT_PIPELINE_BY_PATH.get(db_path)
    if cached is not None:
        return cached
    pipe = dlt_module.pipeline(
        pipeline_name="polymarket_selected_scope_raw",
        destination=dlt_module.destinations.duckdb(credentials=db_path),
        dataset_name="polymarket_raw",
    )
    _DLT_PIPELINE_BY_PATH[db_path] = pipe
    return pipe


__all__ = [
    "_DLT_PIPELINE_BY_PATH",
    "_build_odds_sync_kwargs",
    "_materialize_odds_sync",
    "_odds_sync_metadata",
    "_raw_snapshot_metadata",
    "_run_with_guardrail_thread",
    "_run_with_raw_snapshot",
    "get_polymarket_dlt_pipeline",
]
