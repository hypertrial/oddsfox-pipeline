from __future__ import annotations

import os
from typing import Any, Callable

import dlt
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_KALSHI, schema_name
from oddsfox_pipeline.orchestration import kalshi_ops as ops
from oddsfox_pipeline.orchestration.config import KalshiHourlyOddsSyncConfig
from oddsfox_pipeline.storage.duckdb.connection import active_duckdb_path
from oddsfox_pipeline.storage.duckdb.observability import (
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


_DLT_PIPELINE_BY_PATH: dict[str, dlt.Pipeline] = {}


def _dlt_pipeline_name(dataset_name: str) -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        return f"{dataset_name}_{worker}_landing"
    return f"{dataset_name}_landing"


def get_kalshi_dlt_pipeline(
    *,
    scope_name: str = SCOPE_WC2026,
    active_duckdb_path_fn: Callable[[], Any] = active_duckdb_path,
    dlt_module: Any = dlt,
) -> dlt.Pipeline:
    db_path = str(active_duckdb_path_fn())
    dataset_name = schema_name(SOURCE_KALSHI, scope_name, "raw")
    cache_key = f"{db_path}:{dataset_name}"
    cached = _DLT_PIPELINE_BY_PATH.get(cache_key)
    if cached is not None:
        return cached
    pipe = dlt_module.pipeline(
        pipeline_name=_dlt_pipeline_name(dataset_name),
        destination=dlt_module.destinations.duckdb(credentials=db_path),
        dataset_name=dataset_name,
    )
    _DLT_PIPELINE_BY_PATH[cache_key] = pipe
    return pipe


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

    def _progress(phase: str, payload: dict[str, Any]) -> None:
        work = int(payload.get("markets_synced") or payload.get("rows_written") or 0)
        guardrail.record_progress(
            work_increment=max(0, work),
            phase=phase,
            diagnostics=payload,
        )
        guardrail.check(phase=phase, diagnostics=payload)

    def _run(_pre: dict[str, Any]) -> dict[str, Any]:
        return sync_fn(
            scope_name=scope_name,
            window_hours=config.window_hours,
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


__all__ = [
    "_DLT_PIPELINE_BY_PATH",
    "_raw_snapshot_metadata",
    "_run_with_raw_snapshot",
    "get_kalshi_dlt_pipeline",
    "materialize_kalshi_candlesticks_sync",
]
