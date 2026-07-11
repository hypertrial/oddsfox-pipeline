from __future__ import annotations

import os
from typing import Any, Callable

import dlt
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from oddsfox_pipeline.naming import (
    SCOPE_WC2026,
    schema_name,
)
from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.config import OddsSyncConfig
from oddsfox_pipeline.storage.duckdb.connection import active_duckdb_path
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    format_raw_snapshot_log,
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
    run_summary, _, _, _, raw_metadata = run_with_raw_snapshot_fn(
        config.raw_snapshot_level,
        lambda _pre: sync_odds_fn(**sync_kwargs),
    )
    metadata = _odds_sync_metadata(config, run_summary, raw_metadata)
    return MaterializeResult(metadata=metadata)


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

    def _markets_progress(phase: str, payload: dict[str, Any]) -> None:
        work = int(
            payload.get("events_pages")
            or payload.get("api_requests")
            or payload.get("markets_fetched")
            or 0
        )
        guardrail.record_progress(
            work_increment=max(0, work),
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
        max_event_pages=config.max_event_pages,
        max_pages_without_progress=config.max_pages_without_progress,
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


def _materialize_market_scope_registry(
    context: AssetExecutionContext,
    config: Any,
    *,
    scope_name: str,
    get_sync_run_metrics_fn: Callable[..., dict[str, Any] | None],
    snapshot_refreshed_scope_name_fn: Callable[[dict[str, Any]], str | None],
    sync_market_scope_registry_fn: Callable[..., dict[str, Any]],
    snapshot_raw_layer_fn: Callable[..., dict[str, Any]] = snapshot_raw_layer,
    delta_raw_layer_fn: Callable[
        [dict[str, Any], dict[str, Any]], dict[str, Any]
    ] = delta_raw_layer,
) -> MaterializeResult:
    def _registry_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)

    if config.skip_if_snapshot_refreshed and not config.force_refresh:
        snapshot_metrics = get_sync_run_metrics_fn("sync_markets")
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
                "Skipping market-scope registry refresh; snapshot already refreshed registry"
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
        return sync_market_scope_registry_fn(
            scope_name=scope_name,
            max_event_pages=config.max_event_pages,
            max_pages_without_progress=config.max_pages_without_progress,
            keyset_closed=config.keyset_closed,
            keyset_tag_slugs=config.keyset_tag_slugs,
            keyset_volume_min=config.keyset_volume_min,
            progress_callback=_registry_progress,
        )

    run_summary, _, _, _, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        _sync_registry,
        snapshot_raw_layer_fn=snapshot_raw_layer_fn,
        delta_raw_layer_fn=delta_raw_layer_fn,
    )
    return MaterializeResult(metadata=raw_metadata)


def _materialize_metadata_backfill(
    context: AssetExecutionContext,
    config: Any,
    *,
    asset_name: str,
    scope_name: str,
    backfill_market_metadata_fn: Callable[..., dict[str, Any]],
    delete_orphan_market_tokens_fn: Callable[..., int],
    snapshot_raw_layer_fn: Callable[..., dict[str, Any]] = snapshot_raw_layer,
    delta_raw_layer_fn: Callable[
        [dict[str, Any], dict[str, Any]], dict[str, Any]
    ] = delta_raw_layer,
) -> MaterializeResult:
    guardrail = ops.ProgressGuardrail(
        asset=asset_name,
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_batches,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={
            "batch_size": config.batch_size,
            "max_markets": config.max_markets,
        },
        force_log=True,
    )

    def _metadata_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)
        guardrail.record_progress(work_increment=1, phase=phase, diagnostics=payload)

    pre = snapshot_raw_layer_fn(level=config.raw_snapshot_level)
    backfill_summaries = [
        _run_with_guardrail_thread(
            guardrail,
            "backfill_market_metadata",
            lambda: backfill_market_metadata_fn(
                batch_size=config.batch_size,
                max_markets=config.max_markets,
                force=config.force,
                include_tokens=True,
                include_slugs=config.include_slugs,
                include_event_slugs=config.include_event_slugs,
                include_end_dates=config.include_end_dates,
                progress_callback=_metadata_progress,
                progress_every_n_batches=config.progress_log_interval_batches,
                gamma_requests_per_second=config.gamma_requests_per_second,
                market_scope=scope_name,
                event_slug_fallback_max_pages=config.event_slug_fallback_max_pages,
                event_slug_fallback_max_pages_without_progress=config.event_slug_fallback_max_pages_without_progress,
                event_slug_fallback_progress_every_pages=config.event_slug_fallback_progress_pages,
            ),
            poll_seconds=config.progress_poll_seconds,
            thread_factory=ops.Thread,
        )
    ]
    orphan_market_tokens_removed = delete_orphan_market_tokens_fn(scope_name=scope_name)
    if orphan_market_tokens_removed:
        context.log.info(
            "Removed %s orphan market_tokens row(s) (market_id not in markets) after metadata backfill",
            orphan_market_tokens_removed,
        )
    post = snapshot_raw_layer_fn(level=config.raw_snapshot_level)
    delta = delta_raw_layer_fn(pre, post)
    return MaterializeResult(
        metadata={
            "batch_size": MetadataValue.int(config.batch_size),
            **_raw_snapshot_metadata(pre, post, delta),
            "backfill_summaries": MetadataValue.json(backfill_summaries),
            "orphan_market_tokens_removed": MetadataValue.int(
                orphan_market_tokens_removed
            ),
        }
    )


_DLT_PIPELINE_BY_PATH: dict[str, dlt.Pipeline] = {}


def _dlt_pipeline_name(dataset_name: str) -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        return f"{dataset_name}_{worker}_landing"
    return f"{dataset_name}_landing"


def get_polymarket_dlt_pipeline(
    *,
    scope_name: str = SCOPE_WC2026,
    active_duckdb_path_fn: Callable[[], Any] = active_duckdb_path,
    dlt_module: Any = dlt,
) -> dlt.Pipeline:
    db_path = str(active_duckdb_path_fn())
    dataset_name = schema_name("polymarket", scope_name, "raw")
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


__all__ = [
    "_DLT_PIPELINE_BY_PATH",
    "_build_odds_sync_kwargs",
    "_materialize_market_scope_registry",
    "_materialize_metadata_backfill",
    "_materialize_odds_sync",
    "_materialize_raw_markets_snapshot",
    "_odds_sync_metadata",
    "_raw_snapshot_metadata",
    "_run_raw_markets",
    "_run_with_guardrail_thread",
    "_run_with_raw_snapshot",
    "get_polymarket_dlt_pipeline",
]
