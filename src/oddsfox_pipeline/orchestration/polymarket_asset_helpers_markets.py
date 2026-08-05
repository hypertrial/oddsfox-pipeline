"""Polymarket raw-markets asset helpers."""

from __future__ import annotations

from typing import Any, Callable

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
)
from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.failure_metrics import save_asset_failure_metrics
from oddsfox_pipeline.orchestration.raw_snapshot_helpers import _run_with_raw_snapshot
from oddsfox_pipeline.orchestration.transient_retry import raise_retry_if_transient
from oddsfox_pipeline.storage.duckdb.connection import active_duckdb_path
from oddsfox_pipeline.storage.duckdb.dlt_batch import get_polymarket_dlt_pipeline
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    format_raw_snapshot_log,
    snapshot_raw_layer,
)


def _run_raw_markets(
    context: AssetExecutionContext,
    config: Any,
    dlt_resource: Any,
    *,
    asset_name: str,
    scope_name: str,
    discovery_mode: str,
    source_fn: Callable[..., Any],
    collect_market_scope_payload_fn: Callable[..., dict[str, Any]],
    save_market_tokens_batch_fn: Callable[..., Any],
    save_sync_run_metrics_fn: Callable[..., Any],
    get_connection_fn: Callable[[], Any],
    ensure_indexes_fn: Callable[..., Any],
    active_duckdb_path_fn: Callable[[], Any] = active_duckdb_path,
) -> Any:
    guardrail = ops.ProgressGuardrail(
        asset=asset_name,
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_pages,
    )

    last_work = 0

    def _markets_progress(phase: str, payload: dict[str, Any]) -> None:
        nonlocal last_work
        work = int(
            payload.get("events_page")
            or payload.get("events_pages")
            or payload.get("api_requests")
            or payload.get("markets_fetched")
            or 0
        )
        increment = max(0, work - last_work)
        last_work = work
        guardrail.record_progress(
            work_increment=increment,
            phase=phase,
            diagnostics=payload,
        )
        guardrail.check(phase=phase, diagnostics=payload)

    context.log.info(
        "%s start (discovery_mode=%s, progress_log_interval_pages=%s, progress_log_interval_seconds=%s, no_progress_soft_timeout_seconds=%s, no_progress_hard_timeout_seconds=%s)",
        asset_name,
        config.discovery_mode,
        config.progress_log_interval_pages,
        config.progress_log_interval_seconds,
        config.no_progress_soft_timeout_seconds,
        config.no_progress_hard_timeout_seconds,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={
            "mode": "market_scope_event_first",
            "scope_name": scope_name,
            "discovery_mode": discovery_mode,
        },
        force_log=True,
    )
    try:
        pipeline = get_polymarket_dlt_pipeline(
            scope_name=scope_name,
            active_duckdb_path_fn=active_duckdb_path_fn,
            dlt_module=dlt_resource,
        )
        if pipeline.has_pending_data:
            package_label = asset_name.removesuffix("_markets")
            context.log.info(
                "Clearing pending dlt packages for %s before extract",
                package_label,
            )
            pipeline.drop_pending_packages()
        collection = collect_market_scope_payload_fn(
            discovery_mode=discovery_mode,
            force_full_discovery=config.force_full_discovery,
            scope_name=scope_name,
            refresh_registry=config.refresh_registry,
            max_event_pages=config.max_event_pages,
            max_pages_without_progress=(
                DEFAULT_MAX_PAGES_WITHOUT_PROGRESS
                if config.max_pages_without_progress is None
                else config.max_pages_without_progress
            ),
            keyset_closed=config.keyset_closed,
            keyset_tag_slugs=config.keyset_tag_slugs,
            keyset_volume_min=config.keyset_volume_min,
            progress_callback=_markets_progress,
        )
        dlt_source = source_fn(rows=collection["market_rows"])
        yield from dlt_resource.run(
            context=context,
            dlt_pipeline=pipeline,
            dlt_source=dlt_source,
        )
        save_market_tokens_batch_fn(collection["token_rows"], scope_name=scope_name)
        run_summary = dict(collection["run_summary"])
        guardrail_snapshot = guardrail.snapshot()
        run_summary.update(
            {
                "soft_warning_count": guardrail_snapshot.get("soft_warning_count", 0),
                "max_idle_seconds": guardrail_snapshot.get("max_idle_seconds", 0.0),
            }
        )
        save_sync_run_metrics_fn("sync_markets", run_summary, scope_name=scope_name)
        guardrail.record_progress(
            work_increment=0,
            phase="sync_markets_complete",
            diagnostics={
                "total_fetched": run_summary.get("total_fetched"),
                "aborted": run_summary.get("aborted", False),
            },
            force_log=True,
        )
        with get_connection_fn() as conn:
            ensure_indexes_fn(conn, scope_name=scope_name)
    except Exception as exc:
        save_asset_failure_metrics(
            "sync_markets",
            exc,
            scope_name=scope_name,
        )
        raise_retry_if_transient(exc)
        raise


def _materialize_raw_markets_snapshot(
    context: AssetExecutionContext,
    config: Any,
    *,
    asset_name: str,
    scope_name: str,
    source: str,
    snapshot_raw_layer_fn: Callable[..., dict[str, Any]] = snapshot_raw_layer,
    delta_raw_layer_fn: Callable[
        [dict[str, Any], dict[str, Any]], dict[str, Any]
    ] = delta_raw_layer,
    format_raw_snapshot_log_fn: Callable[
        [dict[str, Any]], str
    ] = format_raw_snapshot_log,
) -> MaterializeResult:
    context.log.info("%s start (local snapshot only)", asset_name)

    def _local_snapshot(pre: dict[str, Any]) -> dict[str, Any]:
        context.log.info("DuckDB pre-run state: %s", format_raw_snapshot_log_fn(pre))
        return {
            "task": "raw_markets_snapshot",
            "mode": "local_snapshot",
            "scope_name": scope_name,
            "skipped_external_discovery": True,
        }

    run_summary, _, _, raw_delta, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        _local_snapshot,
        snapshot_raw_layer_fn=snapshot_raw_layer_fn,
        delta_raw_layer_fn=delta_raw_layer_fn,
    )
    context.log.info("DuckDB delta after %s: %s", asset_name, raw_delta)
    context.log.info("Run summary for raw markets local snapshot: %s", run_summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text(source),
            **raw_metadata,
        }
    )


__all__ = ["_materialize_raw_markets_snapshot", "_run_raw_markets"]
