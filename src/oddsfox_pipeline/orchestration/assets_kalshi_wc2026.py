from typing import Any

import dlt
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets

from oddsfox_pipeline.config.settings import DEFAULT_KALSHI_WC2026_MARKET_SCOPE
from oddsfox_pipeline.ingestion.kalshi.dlt_source import kalshi_wc2026_source
from oddsfox_pipeline.ingestion.kalshi.markets.sync import collect_market_scope_payload
from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_KALSHI, asset_key
from oddsfox_pipeline.orchestration import kalshi_asset_helpers as asset_helpers
from oddsfox_pipeline.orchestration import kalshi_ops as ops
from oddsfox_pipeline.orchestration.config import (
    KalshiHourlyOddsSyncConfig,
    KalshiMarketScopeRegistryConfig,
    KalshiMarketsSyncConfig,
)
from oddsfox_pipeline.orchestration.snapshot_helpers import (
    _snapshot_refreshed_scope_name,
)
from oddsfox_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    get_connection,
)
from oddsfox_pipeline.storage.duckdb.metadata import (
    get_sync_run_metrics,
    save_sync_run_metrics,
)
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    format_raw_snapshot_log,
    snapshot_raw_layer,
)
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import ensure_kalshi_indexes

KALSHI_WC2026_SCOPE_NAME = DEFAULT_KALSHI_WC2026_MARKET_SCOPE
KALSHI_WC2026_RAW_MARKETS = asset_key(SOURCE_KALSHI, SCOPE_WC2026, "raw", "markets")
KALSHI_WC2026_RAW_EVENTS = asset_key(SOURCE_KALSHI, SCOPE_WC2026, "raw", "events")
KALSHI_WC2026_RAW_MARKETS_SNAPSHOT = asset_key(
    SOURCE_KALSHI, SCOPE_WC2026, "raw", "markets_snapshot"
)
KALSHI_WC2026_OPS_MARKET_SCOPE_REGISTRY = asset_key(
    SOURCE_KALSHI, SCOPE_WC2026, "ops", "market_scope_registry"
)
KALSHI_WC2026_RAW_MARKET_CANDLESTICKS_HOURLY = asset_key(
    SOURCE_KALSHI, SCOPE_WC2026, "raw", "market_candlesticks_hourly"
)


class KalshiWc2026DltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data):
        spec = super().get_asset_spec(data)
        resource = data.resource
        if resource.source_name == "kalshi_wc2026" and resource.name == "events":
            return spec.replace_attributes(
                key=KALSHI_WC2026_RAW_EVENTS,
                deps=[],
            )
        if resource.source_name == "kalshi_wc2026" and resource.name == "markets":
            return spec.replace_attributes(
                key=KALSHI_WC2026_RAW_MARKETS,
                deps=[],
            )
        return spec


_KALSHI_DLT_PIPELINE = asset_helpers.get_kalshi_dlt_pipeline(
    active_duckdb_path_fn=active_duckdb_path,
    dlt_module=dlt,
)


@dlt_assets(
    name="kalshi_wc2026_raw_markets",
    group_name="ingestion",
    dlt_source=kalshi_wc2026_source(),
    dlt_pipeline=_KALSHI_DLT_PIPELINE,
    dagster_dlt_translator=KalshiWc2026DltTranslator(),
)
def kalshi_wc2026_raw_markets(
    context: AssetExecutionContext,
    config: KalshiMarketsSyncConfig,
    dlt: DagsterDltResource,
):
    guardrail = ops.ProgressGuardrail(
        asset="kalshi_wc2026_raw_markets",
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_pages,
    )

    def _markets_progress(phase: str, payload: dict[str, Any]) -> None:
        work = int(
            payload.get("pages")
            or payload.get("api_requests")
            or payload.get("markets_collected")
            or 0
        )
        guardrail.record_progress(
            work_increment=max(0, work),
            phase=phase,
            diagnostics=payload,
        )
        guardrail.check(phase=phase, diagnostics=payload)

    context.log.info("kalshi_wc2026_raw_markets start")
    pipeline = asset_helpers.get_kalshi_dlt_pipeline(
        scope_name=KALSHI_WC2026_SCOPE_NAME,
        active_duckdb_path_fn=active_duckdb_path,
        dlt_module=dlt,
    )
    if pipeline.has_pending_data:
        context.log.info(
            "Clearing pending dlt packages for kalshi_wc2026_raw before extract"
        )
        pipeline.drop_pending_packages()
    collection = collect_market_scope_payload(
        scope_name=KALSHI_WC2026_SCOPE_NAME,
        progress_callback=_markets_progress,
    )
    dlt_source = kalshi_wc2026_source(
        events=collection["events"],
        markets=collection["markets"],
    )
    yield from dlt.run(context=context, dlt_pipeline=pipeline, dlt_source=dlt_source)
    run_summary = {
        "scope_name": collection["scope_name"],
        "total_events": collection["total_events"],
        "total_markets": collection["total_markets"],
        "registry_refreshed": True,
        "registry_summary": collection["registry_summary"],
    }
    save_sync_run_metrics(
        "sync_kalshi_markets",
        run_summary,
        scope_name=KALSHI_WC2026_SCOPE_NAME,
        source="kalshi",
    )
    with get_connection() as conn:
        ensure_kalshi_indexes(conn, scope_name=KALSHI_WC2026_SCOPE_NAME)


@multi_asset(
    name="kalshi_wc2026_raw_markets_snapshot",
    specs=[
        AssetSpec(
            key=KALSHI_WC2026_RAW_MARKETS_SNAPSHOT,
            deps=[KALSHI_WC2026_RAW_MARKETS],
        )
    ],
    group_name="ingestion",
)
def kalshi_wc2026_raw_markets_snapshot(
    context: AssetExecutionContext,
    config: KalshiMarketsSyncConfig,
) -> MaterializeResult:
    context.log.info("kalshi_wc2026_raw_markets_snapshot start (local snapshot only)")

    def _local_snapshot(pre: dict[str, Any]) -> dict[str, Any]:
        context.log.info("DuckDB pre-run state: %s", format_raw_snapshot_log(pre))
        return {
            "task": "raw_markets_snapshot",
            "mode": "local_snapshot",
            "scope_name": KALSHI_WC2026_SCOPE_NAME,
            "skipped_external_discovery": True,
        }

    run_summary, _, _, raw_delta, raw_metadata = asset_helpers._run_with_raw_snapshot(
        config.raw_snapshot_level,
        _local_snapshot,
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
    )
    context.log.info(
        "DuckDB delta after kalshi_wc2026_raw_markets_snapshot: %s", raw_delta
    )
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text("external-api.kalshi.com"),
            **raw_metadata,
        }
    )


@multi_asset(
    name="kalshi_wc2026_ops_market_scope_registry",
    specs=[
        AssetSpec(
            key=KALSHI_WC2026_OPS_MARKET_SCOPE_REGISTRY,
            deps=[KALSHI_WC2026_RAW_MARKETS_SNAPSHOT],
        )
    ],
    group_name="ingestion",
)
def kalshi_wc2026_ops_market_scope_registry(
    context: AssetExecutionContext,
    config: KalshiMarketScopeRegistryConfig,
) -> MaterializeResult:
    if config.skip_if_snapshot_refreshed and not config.force_refresh:
        snapshot_metrics = get_sync_run_metrics(
            "sync_kalshi_markets",
            scope_name=KALSHI_WC2026_SCOPE_NAME,
            source="kalshi",
        )
        refreshed_scope_name = (
            _snapshot_refreshed_scope_name(snapshot_metrics)
            if snapshot_metrics
            else None
        )
        if (
            snapshot_metrics
            and snapshot_metrics.get("registry_refreshed") is True
            and refreshed_scope_name == KALSHI_WC2026_SCOPE_NAME
        ):
            context.log.info(
                "Skipping Kalshi market-scope registry refresh; snapshot already refreshed"
            )
            pre = snapshot_raw_layer(level=config.raw_snapshot_level)
            run_summary = {
                "skipped": True,
                "reason": "snapshot_refreshed_registry",
                "scope_name": KALSHI_WC2026_SCOPE_NAME,
                "snapshot_metrics": snapshot_metrics,
            }
            return MaterializeResult(
                metadata=asset_helpers._raw_snapshot_metadata(
                    pre,
                    pre,
                    {},
                    run_summary=run_summary,
                )
            )

    def _sync_registry_wrapped(_pre: dict[str, Any]) -> dict[str, Any]:
        from oddsfox_pipeline.ingestion.kalshi.series_scope.config import (
            load_market_scope_config,
        )

        summary = ops.sync_kalshi_market_scope_registry(
            config=load_market_scope_config(scope_name=KALSHI_WC2026_SCOPE_NAME),
        )
        return {"scope_name": KALSHI_WC2026_SCOPE_NAME, **summary}

    run_summary, _, _, raw_delta, raw_metadata = asset_helpers._run_with_raw_snapshot(
        config.raw_snapshot_level,
        _sync_registry_wrapped,
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
    )
    context.log.info("Kalshi registry refresh delta: %s", raw_delta)
    return MaterializeResult(metadata=raw_metadata)


@multi_asset(
    name="kalshi_wc2026_raw_market_candlesticks_hourly",
    specs=[
        AssetSpec(
            key=KALSHI_WC2026_RAW_MARKET_CANDLESTICKS_HOURLY,
            deps=[KALSHI_WC2026_OPS_MARKET_SCOPE_REGISTRY],
        )
    ],
    group_name="ingestion",
)
def kalshi_wc2026_raw_market_candlesticks_hourly(
    context: AssetExecutionContext,
    config: KalshiHourlyOddsSyncConfig,
) -> MaterializeResult:
    return asset_helpers.materialize_kalshi_candlesticks_sync(
        context,
        config,
        scope_name=KALSHI_WC2026_SCOPE_NAME,
    )


__all__ = [
    "KALSHI_WC2026_OPS_MARKET_SCOPE_REGISTRY",
    "KALSHI_WC2026_RAW_MARKET_CANDLESTICKS_HOURLY",
    "KALSHI_WC2026_RAW_MARKETS",
    "KALSHI_WC2026_RAW_MARKETS_SNAPSHOT",
    "kalshi_wc2026_ops_market_scope_registry",
    "kalshi_wc2026_raw_market_candlesticks_hourly",
    "kalshi_wc2026_raw_markets",
    "kalshi_wc2026_raw_markets_snapshot",
]
