from __future__ import annotations

from typing import Any, Callable

import dlt
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_KALSHI, schema_name
from oddsfox_pipeline.orchestration import kalshi_ops as ops
from oddsfox_pipeline.orchestration.config import KalshiHourlyOddsSyncConfig
from oddsfox_pipeline.orchestration.raw_snapshot_helpers import (
    _DLT_PIPELINE_BY_PATH,
    _raw_snapshot_metadata,
    _run_with_raw_snapshot,
    get_cached_dlt_pipeline,
)
from oddsfox_pipeline.storage.duckdb.connection import active_duckdb_path
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    snapshot_raw_layer,
)


def get_kalshi_dlt_pipeline(
    *,
    scope_name: str = SCOPE_WC2026,
    active_duckdb_path_fn: Callable[[], Any] = active_duckdb_path,
    dlt_module: Any = dlt,
) -> dlt.Pipeline:
    dataset_name = schema_name(SOURCE_KALSHI, scope_name, "raw")
    return get_cached_dlt_pipeline(
        dataset_name=dataset_name,
        active_duckdb_path_fn=active_duckdb_path_fn,
        dlt_module=dlt_module,
    )


def materialize_kalshi_candlesticks_sync(
    context: AssetExecutionContext,
    config: KalshiHourlyOddsSyncConfig,
    *,
    scope_name: str,
    sync_fn: Callable[..., dict[str, Any]] = ops.sync_kalshi_candlesticks,
    run_with_raw_snapshot_fn: Callable[..., tuple] = _run_with_raw_snapshot,
) -> MaterializeResult:
    guardrail = ops.ProgressGuardrail(
        asset="kalshi_wc2026_raw_market_candlesticks_hourly",
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_markets,
    )

    last_work = 0

    def _progress(phase: str, payload: dict[str, Any]) -> None:
        nonlocal last_work
        work = int(payload.get("markets_synced") or payload.get("rows_written") or 0)
        increment = max(0, work - last_work)
        last_work = work
        guardrail.record_progress(
            work_increment=increment,
            phase=phase,
            diagnostics=payload,
        )
        guardrail.check(phase=phase, diagnostics=payload)

    def _run(_pre: dict[str, Any]) -> dict[str, Any]:
        return sync_fn(
            scope_name=scope_name,
            window_hours=config.window_hours,
            history_backfill_days=config.history_backfill_days,
            routine_interval_hours=config.routine_interval_hours,
            force=config.force,
            progress_callback=_progress,
        )

    run_summary, _, _, _, raw_metadata = run_with_raw_snapshot_fn(
        config.raw_snapshot_level,
        _run,
    )
    metadata = {
        "window_hours": MetadataValue.int(config.window_hours),
        "force": MetadataValue.bool(config.force),
        "markets_synced": MetadataValue.int(
            int(run_summary.get("markets_synced") or 0)
        ),
        "rows_written": MetadataValue.int(int(run_summary.get("rows_written") or 0)),
        **raw_metadata,
    }
    return MaterializeResult(metadata=metadata)


def _materialize_market_scope_registry(
    context: AssetExecutionContext,
    config: Any,
    *,
    scope_name: str,
    sync_task_name: str,
    get_sync_run_metrics_fn: Callable[..., dict[str, Any] | None],
    snapshot_refreshed_scope_name_fn: Callable[[dict[str, Any]], str | None],
    sync_market_scope_registry_fn: Callable[[], dict[str, Any]],
    snapshot_raw_layer_fn: Callable[..., dict[str, Any]] = snapshot_raw_layer,
    delta_raw_layer_fn: Callable[
        [dict[str, Any], dict[str, Any]], dict[str, Any]
    ] = delta_raw_layer,
) -> MaterializeResult:
    if config.skip_if_snapshot_refreshed and not config.force_refresh:
        snapshot_metrics = get_sync_run_metrics_fn(
            sync_task_name,
            scope_name=scope_name,
        )
        refreshed_scope_name = (
            snapshot_refreshed_scope_name_fn(snapshot_metrics)
            if snapshot_metrics
            else None
        )
        if (
            snapshot_metrics
            and snapshot_metrics.get("registry_refreshed") is True
            and refreshed_scope_name == scope_name
        ):
            context.log.info(
                "Skipping Kalshi market-scope registry refresh; snapshot already refreshed"
            )
            pre = snapshot_raw_layer_fn(level=config.raw_snapshot_level)
            run_summary = {
                "skipped": True,
                "reason": "snapshot_refreshed_registry",
                "scope_name": scope_name,
                "snapshot_metrics": snapshot_metrics,
            }
            return MaterializeResult(
                metadata=_raw_snapshot_metadata(
                    pre,
                    pre,
                    {},
                    run_summary=run_summary,
                )
            )

    def _sync_registry(_pre: dict[str, Any]) -> dict[str, Any]:
        return sync_market_scope_registry_fn()

    run_summary, _, _, raw_delta, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        _sync_registry,
        snapshot_raw_layer_fn=snapshot_raw_layer_fn,
        delta_raw_layer_fn=delta_raw_layer_fn,
    )
    context.log.info("Kalshi registry refresh delta: %s", raw_delta)
    return MaterializeResult(metadata=raw_metadata)


__all__ = [
    "_DLT_PIPELINE_BY_PATH",
    "_materialize_market_scope_registry",
    "_raw_snapshot_metadata",
    "_run_with_raw_snapshot",
    "get_kalshi_dlt_pipeline",
    "materialize_kalshi_candlesticks_sync",
]
