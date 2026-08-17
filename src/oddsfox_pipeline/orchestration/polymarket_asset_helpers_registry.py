"""Polymarket registry / event-catalog / metadata asset helpers."""

from __future__ import annotations

from typing import Any, Callable

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
)
from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.failure_metrics import save_asset_failure_metrics
from oddsfox_pipeline.orchestration.raw_snapshot_helpers import (
    _raw_snapshot_metadata,
    _run_with_raw_snapshot,
)
from oddsfox_pipeline.orchestration.transient_retry import raise_retry_if_transient
from oddsfox_pipeline.storage.duckdb.metadata import (
    clear_event_catalog_partition_checkpoints,
    load_event_catalog_partition_checkpoints,
    save_event_catalog_partition_checkpoint,
)
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    snapshot_raw_layer,
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
        snapshot_metrics = get_sync_run_metrics_fn(
            "sync_markets",
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
            max_pages_without_progress=(
                DEFAULT_MAX_PAGES_WITHOUT_PROGRESS
                if config.max_pages_without_progress is None
                else config.max_pages_without_progress
            ),
            keyset_closed=config.keyset_closed,
            keyset_tag_slugs=config.keyset_tag_slugs,
            keyset_volume_min=config.keyset_volume_min,
            apply_event_volume_eligibility_gate=(
                config.apply_event_volume_eligibility_gate
            ),
            progress_callback=_registry_progress,
        )

    try:
        run_summary, _, _, _, raw_metadata = _run_with_raw_snapshot(
            config.raw_snapshot_level,
            _sync_registry,
            snapshot_raw_layer_fn=snapshot_raw_layer_fn,
            delta_raw_layer_fn=delta_raw_layer_fn,
        )
    except Exception as exc:
        save_asset_failure_metrics(
            "sync_market_scope_registry",
            exc,
            scope_name=scope_name,
        )
        raise_retry_if_transient(exc)
        raise
    return MaterializeResult(metadata=raw_metadata)


def _materialize_metadata_enrichment(
    context: AssetExecutionContext,
    config: Any,
    *,
    asset_name: str,
    scope_name: str,
    enrich_market_metadata_fn: Callable[..., dict[str, Any]],
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
        guardrail.check(phase=phase, diagnostics=payload)

    pre = snapshot_raw_layer_fn(level=config.raw_snapshot_level)
    try:
        backfill_summaries = [
            enrich_market_metadata_fn(
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
            )
        ]
    except Exception as exc:
        save_asset_failure_metrics(
            "metadata_enrichment",
            exc,
            scope_name=scope_name,
        )
        raise_retry_if_transient(exc)
        raise
    orphan_market_tokens_removed = delete_orphan_market_tokens_fn(scope_name=scope_name)
    if orphan_market_tokens_removed:
        context.log.info(
            "Removed %s orphan market_tokens row(s) (market_id not in markets) after metadata enrichment",
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


def _materialize_event_catalog(
    context: AssetExecutionContext,
    config: Any,
    *,
    asset_name: str,
    scope_name: str,
    collect_event_catalog_fn: Callable[..., Any],
    merge_event_catalog_batch_fn: Callable[..., Any],
    normalize_market_payloads_fn: Callable[..., Any],
    ensure_indexes_fn: Callable[..., Any],
    get_connection_fn: Callable[[], Any],
    save_sync_run_metrics_fn: Callable[..., Any],
    event_catalog_key: Any,
    event_snapshots_key: Any,
    event_memberships_key: Any,
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

    def _catalog_progress(phase: str, payload: dict[str, Any]) -> None:
        nonlocal last_work
        work = int(payload.get("events_page") or 0)
        increment = max(0, work - last_work)
        last_work = work
        guardrail.record_progress(
            work_increment=increment,
            phase=phase,
            diagnostics=payload,
        )
        guardrail.check(phase=phase, diagnostics=payload)

    context.log.info(
        "%s start (max_event_pages=%s, include_slug_prefix_recall=%s, "
        "slug_prefix_recall_max_pages_without_progress=%s, "
        "progress_log_interval_pages=%s, progress_log_interval_seconds=%s, "
        "no_progress_soft_timeout_seconds=%s, no_progress_hard_timeout_seconds=%s)",
        asset_name,
        config.max_event_pages,
        getattr(config, "include_slug_prefix_recall", False),
        getattr(config, "slug_prefix_recall_max_pages_without_progress", None),
        config.progress_log_interval_pages,
        config.progress_log_interval_seconds,
        config.no_progress_soft_timeout_seconds,
        config.no_progress_hard_timeout_seconds,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={"scope_name": scope_name},
        force_log=True,
    )

    def _load_checkpoints() -> dict[str, dict[str, Any]]:
        with get_connection_fn() as conn:
            return load_event_catalog_partition_checkpoints(conn, scope_name=scope_name)

    def _save_checkpoint(
        partition_key: str,
        stable_events: dict[str, dict[str, Any]],
        scan_summary: dict[str, Any],
    ) -> None:
        with get_connection_fn() as conn:
            save_event_catalog_partition_checkpoint(
                conn,
                partition_key,
                stable_events,
                scan_summary,
                scope_name=scope_name,
            )

    def _clear_checkpoints() -> None:
        with get_connection_fn() as conn:
            clear_event_catalog_partition_checkpoints(conn, scope_name=scope_name)

    if getattr(config, "reset_event_catalog_checkpoint", False):
        _clear_checkpoints()

    try:
        batch = collect_event_catalog_fn(
            max_pages=config.max_event_pages,
            progress_callback=_catalog_progress,
            include_slug_prefix_recall=getattr(
                config, "include_slug_prefix_recall", False
            ),
            slug_prefix_recall_max_pages_without_progress=getattr(
                config, "slug_prefix_recall_max_pages_without_progress", None
            ),
            load_checkpoint_fn=_load_checkpoints,
            save_checkpoint_fn=_save_checkpoint,
        )
        market_rows = normalize_market_payloads_fn(
            batch.market_payloads,
            observed_at=batch.summary["observed_at"],
        )
        with get_connection_fn() as conn:
            merge_event_catalog_batch_fn(
                event_rows=batch.event_snapshots,
                tag_rows=batch.event_tag_snapshots,
                event_market_rows=batch.event_market_snapshots,
                market_rows=market_rows,
                conn=conn,
            )
            ensure_indexes_fn(conn, scope_name=scope_name)
        # Clear recovery checkpoints only after the warehouse merge succeeds.
        _clear_checkpoints()
        guardrail_snapshot = guardrail.snapshot()
        summary = dict(batch.summary)
        summary.update(
            {
                "soft_warning_count": guardrail_snapshot.get("soft_warning_count", 0),
                "max_idle_seconds": guardrail_snapshot.get("max_idle_seconds", 0.0),
            }
        )
        save_sync_run_metrics_fn("event_catalog", summary, scope_name=scope_name)
        context.log.info("%s: %s", asset_name, summary)
    except Exception as exc:
        save_asset_failure_metrics(
            "event_catalog",
            exc,
            scope_name=scope_name,
        )
        raise_retry_if_transient(exc)
        raise

    yield MaterializeResult(
        asset_key=event_snapshots_key,
        metadata={
            "events": len(batch.event_snapshots),
            "event_tags": len(batch.event_tag_snapshots),
            "observed_at": batch.summary["observed_at"],
        },
    )
    yield MaterializeResult(
        asset_key=event_memberships_key,
        metadata={
            "event_markets": len(batch.event_market_snapshots),
            "unique_markets": len(batch.market_payloads),
            "observed_at": batch.summary["observed_at"],
        },
    )
    yield MaterializeResult(
        asset_key=event_catalog_key,
        metadata=batch.summary,
    )


__all__ = [
    "_materialize_event_catalog",
    "_materialize_market_scope_registry",
    "_materialize_metadata_enrichment",
]
