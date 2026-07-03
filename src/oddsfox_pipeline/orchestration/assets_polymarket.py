from typing import Any, Callable

import dlt
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets

from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    collect_raw_markets,
    normalize_market_payloads_for_dlt,
    polymarket_markets_source,
)
from oddsfox_pipeline.orchestration import polymarket_asset_helpers as asset_helpers
from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.config import (
    DbtBuildConfig,
    HourlyOddsSyncConfig,
    MarketScopeRegistryConfig,
    MarketsSyncConfig,
    MetadataBackfillConfig,
    MinutelyOddsSyncConfig,
    OddsSyncConfig,
    RepairConfig,
)
from oddsfox_pipeline.orchestration.dbt_project import DBT_PROJECT
from oddsfox_pipeline.orchestration.translators import PolymarketDagsterDbtTranslator
from oddsfox_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    get_connection,
)
from oddsfox_pipeline.storage.duckdb.metadata import get_sync_run_metrics
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import ensure_polymarket_indexes

_DLT_PIPELINE_BY_PATH = asset_helpers._DLT_PIPELINE_BY_PATH


class Wc2026PolymarketDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data):
        spec = super().get_asset_spec(data)
        resource = data.resource
        if resource.source_name == "wc2026_polymarket" and resource.name == "markets":
            return spec.replace_attributes(
                key=AssetKey("wc2026_polymarket_raw_markets"),
                deps=[],
            )
        return (
            spec  # pragma: no cover - current WC2026 dlt source exposes only markets.
        )


def _merge_scope_sync_summaries(per_scope: list[dict[str, Any]]) -> dict[str, Any]:
    if not per_scope:
        return {
            "task": "sync_markets",
            "scope_names": [],
            "per_scope": [],
            "total_fetched": 0,
        }
    summary = dict(per_scope[0])
    summary["scope_names"] = [summary.get("scope_name")]
    summary["per_scope"] = per_scope
    return summary


def _snapshot_refreshed_scope_names(snapshot_metrics: dict[str, Any]) -> list[str]:
    scope_names = snapshot_metrics.get("scope_names")
    if isinstance(scope_names, list) and scope_names:
        return [str(scope) for scope in scope_names]
    scope_name = snapshot_metrics.get("scope_name")
    return [str(scope_name)] if scope_name else []


def _run_with_raw_snapshot(
    raw_snapshot_level: str,
    run_fn: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict]:
    return asset_helpers._run_with_raw_snapshot(
        raw_snapshot_level,
        run_fn,
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
    )


def _raw_snapshot_metadata(
    pre: dict[str, Any],
    post: dict[str, Any],
    delta: dict[str, Any],
    *,
    run_summary: dict[str, Any] | None = None,
) -> dict[str, MetadataValue]:
    return asset_helpers._raw_snapshot_metadata(
        pre,
        post,
        delta,
        run_summary=run_summary,
    )


def _build_odds_sync_kwargs(
    config: OddsSyncConfig,
    progress_callback: Callable[[str, dict[str, Any]], None],
    *,
    plan_iterator_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    return asset_helpers._build_odds_sync_kwargs(
        config,
        progress_callback,
        plan_iterator_factory=plan_iterator_factory,
    )


def _odds_sync_metadata(
    config: OddsSyncConfig,
    run_summary: dict[str, Any],
    raw_metadata: dict[str, MetadataValue],
) -> dict[str, MetadataValue]:
    return asset_helpers._odds_sync_metadata(config, run_summary, raw_metadata)


def _run_with_guardrail_thread(
    guardrail: Any,
    phase_name: str,
    run_fn: Callable[[], dict[str, Any]],
    *,
    poll_seconds: float,
) -> dict[str, Any]:
    return asset_helpers._run_with_guardrail_thread(
        guardrail,
        phase_name,
        run_fn,
        poll_seconds=poll_seconds,
        thread_factory=ops.Thread,
    )


def _materialize_odds_sync(
    context: AssetExecutionContext,
    config: OddsSyncConfig,
    *,
    plan_iterator_factory: Callable[..., Any] | None = None,
) -> MaterializeResult:
    return asset_helpers._materialize_odds_sync(
        context,
        config,
        plan_iterator_factory=plan_iterator_factory,
        sync_odds_fn=ops.sync_odds,
        run_with_raw_snapshot_fn=_run_with_raw_snapshot,
    )


def get_polymarket_dlt_pipeline() -> dlt.Pipeline:
    return asset_helpers.get_polymarket_dlt_pipeline(
        active_duckdb_path_fn=active_duckdb_path,
        dlt_module=dlt,
    )


_POLYMARKET_DLT_PIPELINE = get_polymarket_dlt_pipeline()


@dlt_assets(
    name="wc2026_polymarket_raw_markets",
    group_name="ingestion",
    dlt_source=polymarket_markets_source(),
    dlt_pipeline=_POLYMARKET_DLT_PIPELINE,
    dagster_dlt_translator=Wc2026PolymarketDltTranslator(),
)
def wc2026_polymarket_raw_markets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    pipeline = get_polymarket_dlt_pipeline()
    if pipeline.has_pending_data:
        context.log.info(
            "Clearing pending dlt packages for wc2026_polymarket_raw before extract"
        )
        pipeline.drop_pending_packages()
    rows = normalize_market_payloads_for_dlt(collect_raw_markets())
    dlt_source = polymarket_markets_source(rows=rows)
    yield from dlt.run(context=context, dlt_pipeline=pipeline, dlt_source=dlt_source)
    with get_connection() as conn:
        ensure_polymarket_indexes(conn)


@asset(
    name="wc2026_polymarket_markets_snapshot",
    deps=["wc2026_polymarket_raw_markets"],
    group_name="ingestion",
)
def wc2026_polymarket_markets_snapshot(
    context: AssetExecutionContext,
    config: MarketsSyncConfig,
) -> MaterializeResult:
    guardrail = ops.ProgressGuardrail(
        asset="wc2026_polymarket_markets_snapshot",
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_pages,
    )

    def _markets_progress(phase: str, payload: dict[str, Any]) -> None:
        guardrail.record_progress(
            work_increment=1,
            phase=phase,
            diagnostics=payload,
        )

    context.log.info(
        "wc2026_polymarket_markets_snapshot start (discovery_mode=%s, progress_log_interval_pages=%s, progress_log_interval_seconds=%s, no_progress_soft_timeout_seconds=%s, no_progress_hard_timeout_seconds=%s)",
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
            "scope_names": config.scope_names,
            "discovery_mode": config.discovery_mode,
        },
        force_log=True,
    )

    def _sync_markets(pre: dict[str, Any]) -> dict[str, Any]:
        context.log.info("DuckDB pre-run state: %s", format_raw_snapshot_log(pre))
        per_scope = [
            ops.sync_markets(
                discovery_mode=config.discovery_mode,
                force_full_discovery=config.force_full_discovery,
                scope_name=scope_name,
                max_event_pages=config.max_event_pages,
                max_pages_without_progress=config.max_pages_without_progress,
                keyset_closed=config.keyset_closed,
                keyset_tag_slugs=config.keyset_tag_slugs,
                keyset_volume_min=config.keyset_volume_min,
                progress_callback=_markets_progress,
                progress_log_interval_pages=config.progress_log_interval_pages,
                progress_log_interval_seconds=config.progress_log_interval_seconds,
                no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
                no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
                progress_poll_seconds=config.progress_poll_seconds,
            )
            for scope_name in config.scope_names
        ]
        return _merge_scope_sync_summaries(per_scope)

    run_summary, _, _, raw_delta, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        _sync_markets,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="sync_markets_complete",
        diagnostics={
            "total_fetched": run_summary.get("total_fetched"),
            "aborted": run_summary.get("aborted", False),
        },
        force_log=True,
    )
    context.log.info(
        "DuckDB delta after wc2026_polymarket_markets_snapshot: %s", raw_delta
    )
    context.log.info("Run summary for sync_markets: %s", run_summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text("gamma-api.polymarket.com"),
            **raw_metadata,
        }
    )


@asset(
    name="wc2026_polymarket_market_registry",
    deps=[wc2026_polymarket_markets_snapshot],
    group_name="ingestion",
)
def wc2026_polymarket_market_registry(
    context: AssetExecutionContext,
    config: MarketScopeRegistryConfig,
) -> MaterializeResult:
    def _registry_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)

    if config.skip_if_snapshot_refreshed and not config.force_refresh:
        snapshot_metrics = get_sync_run_metrics("sync_markets")
        refreshed_scope_names = (
            _snapshot_refreshed_scope_names(snapshot_metrics)
            if snapshot_metrics
            else []
        )
        if (
            snapshot_metrics
            and snapshot_metrics.get("registry_refreshed") is True
            and refreshed_scope_names == config.scope_names
        ):
            context.log.info(
                "Skipping market-scope registry refresh; snapshot already refreshed registry"
            )
            pre = snapshot_raw_layer(level=config.raw_snapshot_level)
            run_summary = {
                "skipped": True,
                "reason": "snapshot_refreshed_registry",
                "scope_names": config.scope_names,
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
        per_scope = [
            ops.sync_market_scope_registry(
                scope_name=scope_name,
                max_event_pages=config.max_event_pages,
                max_pages_without_progress=config.max_pages_without_progress,
                keyset_closed=config.keyset_closed,
                keyset_tag_slugs=config.keyset_tag_slugs,
                keyset_volume_min=config.keyset_volume_min,
                progress_callback=_registry_progress,
            )
            for scope_name in config.scope_names
        ]
        return {
            "scope_names": config.scope_names,
            "per_scope": per_scope,
            "registry_rows_upserted": sum(
                int(summary.get("registry_rows_upserted", 0)) for summary in per_scope
            ),
        }

    run_summary, _, _, _, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        _sync_registry,
    )
    return MaterializeResult(metadata=raw_metadata)


@asset(
    name="wc2026_polymarket_market_metadata_backfill",
    deps=[wc2026_polymarket_market_registry],
    group_name="ingestion",
)
def wc2026_polymarket_market_metadata_backfill(
    context: AssetExecutionContext,
    config: MetadataBackfillConfig,
) -> MaterializeResult:
    guardrail = ops.ProgressGuardrail(
        asset="wc2026_polymarket_market_metadata_backfill",
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

    def _metadata_progress(phase: str, payload: dict) -> None:
        context.log.info("[%s] %s", phase, payload)
        guardrail.record_progress(work_increment=1, phase=phase, diagnostics=payload)

    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    backfill_summaries = [
        _run_with_guardrail_thread(
            guardrail,
            "backfill_market_metadata",
            lambda: ops.backfill_market_metadata(
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
                market_scope=config.scope_names,
                event_slug_fallback_max_pages=config.event_slug_fallback_max_pages,
                event_slug_fallback_max_pages_without_progress=config.event_slug_fallback_max_pages_without_progress,
                event_slug_fallback_progress_every_pages=config.event_slug_fallback_progress_pages,
            ),
            poll_seconds=config.progress_poll_seconds,
        )
    ]
    orphan_market_tokens_removed = ops.delete_orphan_market_tokens()
    if orphan_market_tokens_removed:
        context.log.info(
            "Removed %s orphan market_tokens row(s) (market_id not in markets) after metadata backfill",
            orphan_market_tokens_removed,
        )
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    return MaterializeResult(
        metadata={
            "batch_size": MetadataValue.int(config.batch_size),
            **_raw_snapshot_metadata(pre, post, dd),
            "backfill_summaries": MetadataValue.json(backfill_summaries),
            "orphan_market_tokens_removed": MetadataValue.int(
                orphan_market_tokens_removed
            ),
        }
    )


@asset(
    name="wc2026_polymarket_token_odds_history_minutely",
    deps=[wc2026_polymarket_market_metadata_backfill],
    group_name="ingestion",
)
def wc2026_polymarket_token_odds_history_minutely(
    context: AssetExecutionContext,
    config: MinutelyOddsSyncConfig,
) -> MaterializeResult:
    return _materialize_odds_sync(context, config)


@asset(
    name="wc2026_polymarket_token_odds_history_hourly",
    deps=[wc2026_polymarket_market_metadata_backfill],
    group_name="ingestion",
)
def wc2026_polymarket_token_odds_history_hourly(
    context: AssetExecutionContext,
    config: HourlyOddsSyncConfig,
) -> MaterializeResult:
    return _materialize_odds_sync(context, config)


@asset(name="wc2026_polymarket_odds_repair", group_name="ingestion")
def wc2026_polymarket_odds_repair(
    context: AssetExecutionContext,
    config: RepairConfig,
) -> MaterializeResult:
    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    reconcile_meta = ops.reconcile_odds_ledger(
        persist_run_metrics=config.persist_run_metrics
    )
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    return MaterializeResult(
        metadata={
            "reconcile": MetadataValue.json(reconcile_meta),
            **_raw_snapshot_metadata(pre, post, dd),
        }
    )


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="wc2026_polymarket_dbt",
    dagster_dbt_translator=PolymarketDagsterDbtTranslator(),
)
def wc2026_polymarket_dbt(
    context: AssetExecutionContext, dbt: DbtCliResource, config: DbtBuildConfig
):
    pre_raw = snapshot_raw_layer(level=config.raw_snapshot_level)
    pre_dbt = snapshot_dbt_models()

    yield from ops.stream_dbt_build(
        asset_name="wc2026_polymarket_dbt",
        context=context,
        dbt=dbt,
        config=config,
    )

    post_raw = snapshot_raw_layer(level=config.raw_snapshot_level)
    post_dbt = snapshot_dbt_models()
    context.log.info(
        "DuckDB delta after dbt build (raw tables): %s",
        delta_raw_layer(pre_raw, post_raw),
    )
    context.log.info(
        "dbt model state after build: %s", format_dbt_snapshot_log(post_dbt)
    )
    context.log.info(
        "DuckDB delta after dbt build (dbt models only): %s",
        delta_dbt_models(pre_dbt, post_dbt),
    )


__all__ = [
    "wc2026_polymarket_dbt",
    "wc2026_polymarket_market_metadata_backfill",
    "wc2026_polymarket_raw_markets",
    "wc2026_polymarket_markets_snapshot",
    "wc2026_polymarket_odds_repair",
    "wc2026_polymarket_token_odds_history_hourly",
    "wc2026_polymarket_token_odds_history_minutely",
    "wc2026_polymarket_market_registry",
]
