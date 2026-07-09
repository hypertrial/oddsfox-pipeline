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

from oddsfox_pipeline.config.settings import (
    DEFAULT_POLYMARKET_US_MIDTERMS_2026_MARKET_SCOPE,
)
from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    polymarket_us_midterms_2026_markets_source,
)
from oddsfox_pipeline.ingestion.polymarket.markets.sync import (
    collect_market_scope_payload,
)
from oddsfox_pipeline.naming import SCOPE_US_MIDTERMS_2026, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration import polymarket_asset_helpers as asset_helpers
from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.config import (
    HourlyOddsSyncConfig,
    MarketScopeRegistryConfig,
    MarketsSyncConfig,
    MetadataBackfillConfig,
)
from oddsfox_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    get_connection,
)
from oddsfox_pipeline.storage.duckdb.markets import save_market_tokens_batch
from oddsfox_pipeline.storage.duckdb.metadata import (
    get_sync_run_metrics,
    save_sync_run_metrics,
)
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    format_raw_snapshot_log,
    snapshot_raw_layer,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import ensure_polymarket_indexes

POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME = (
    DEFAULT_POLYMARKET_US_MIDTERMS_2026_MARKET_SCOPE
)
POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS = asset_key(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "markets"
)
POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS_SNAPSHOT = asset_key(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "markets_snapshot"
)
POLYMARKET_US_MIDTERMS_2026_OPS_MARKET_SCOPE_REGISTRY = asset_key(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "ops", "market_scope_registry"
)
POLYMARKET_US_MIDTERMS_2026_RAW_MARKET_METADATA_BACKFILL = asset_key(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "market_metadata_backfill"
)
POLYMARKET_US_MIDTERMS_2026_RAW_TOKEN_ODDS_HISTORY_HOURLY = asset_key(
    SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "token_odds_history_hourly"
)


class PolymarketUsMidterms2026DltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data):
        spec = super().get_asset_spec(data)
        resource = data.resource
        if (
            resource.source_name == "polymarket_us_midterms_2026"
            and resource.name == "markets"
        ):
            return spec.replace_attributes(
                key=POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS,
                deps=[],
            )
        return (
            spec  # pragma: no cover - current WC2026 dlt source exposes only markets.
        )


def _snapshot_refreshed_scope_name(snapshot_metrics: dict[str, Any]) -> str | None:
    scope_name = snapshot_metrics.get("scope_name")
    return str(scope_name) if scope_name else None


_POLYMARKET_DLT_PIPELINE = asset_helpers.get_polymarket_dlt_pipeline(
    scope_name=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
    active_duckdb_path_fn=active_duckdb_path,
    dlt_module=dlt,
)


@dlt_assets(
    name="polymarket_us_midterms_2026_raw_markets",
    group_name="ingestion",
    dlt_source=polymarket_us_midterms_2026_markets_source(),
    dlt_pipeline=_POLYMARKET_DLT_PIPELINE,
    dagster_dlt_translator=PolymarketUsMidterms2026DltTranslator(),
)
def polymarket_us_midterms_2026_raw_markets(
    context: AssetExecutionContext,
    config: MarketsSyncConfig,
    dlt: DagsterDltResource,
):
    guardrail = ops.ProgressGuardrail(
        asset="polymarket_us_midterms_2026_raw_markets",
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
        "polymarket_us_midterms_2026_raw_markets start (discovery_mode=%s, progress_log_interval_pages=%s, progress_log_interval_seconds=%s, no_progress_soft_timeout_seconds=%s, no_progress_hard_timeout_seconds=%s)",
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
            "scope_name": POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
            "discovery_mode": config.discovery_mode,
        },
        force_log=True,
    )
    pipeline = asset_helpers.get_polymarket_dlt_pipeline(
        scope_name=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
        active_duckdb_path_fn=active_duckdb_path,
        dlt_module=dlt,
    )
    if pipeline.has_pending_data:
        context.log.info(
            "Clearing pending dlt packages for polymarket_us_midterms_2026_raw before extract"
        )
        pipeline.drop_pending_packages()
    collection = collect_market_scope_payload(
        discovery_mode="targeted",
        force_full_discovery=config.force_full_discovery,
        scope_name=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
        max_event_pages=config.max_event_pages,
        max_pages_without_progress=config.max_pages_without_progress,
        keyset_closed=config.keyset_closed,
        keyset_tag_slugs=config.keyset_tag_slugs,
        keyset_volume_min=config.keyset_volume_min,
        progress_callback=_markets_progress,
    )
    rows = collection["market_rows"]
    dlt_source = polymarket_us_midterms_2026_markets_source(rows=rows)
    yield from dlt.run(context=context, dlt_pipeline=pipeline, dlt_source=dlt_source)
    save_market_tokens_batch(
        collection["token_rows"], scope_name=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME
    )
    run_summary = dict(collection["run_summary"])
    guardrail_snapshot = guardrail.snapshot()
    run_summary.update(
        {
            "soft_warning_count": guardrail_snapshot.get("soft_warning_count", 0),
            "max_idle_seconds": guardrail_snapshot.get("max_idle_seconds", 0.0),
        }
    )
    save_sync_run_metrics("sync_markets", run_summary)
    guardrail.record_progress(
        work_increment=0,
        phase="sync_markets_complete",
        diagnostics={
            "total_fetched": run_summary.get("total_fetched"),
            "aborted": run_summary.get("aborted", False),
        },
        force_log=True,
    )
    with get_connection() as conn:
        ensure_polymarket_indexes(
            conn, scope_name=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME
        )


@multi_asset(
    name="polymarket_us_midterms_2026_raw_markets_snapshot",
    specs=[
        AssetSpec(
            key=POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS_SNAPSHOT,
            deps=[POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS],
        )
    ],
    group_name="ingestion",
)
def polymarket_us_midterms_2026_raw_markets_snapshot(
    context: AssetExecutionContext,
    config: MarketsSyncConfig,
) -> MaterializeResult:
    context.log.info(
        "polymarket_us_midterms_2026_raw_markets_snapshot start (local snapshot only)"
    )

    def _local_snapshot(pre: dict[str, Any]) -> dict[str, Any]:
        context.log.info("DuckDB pre-run state: %s", format_raw_snapshot_log(pre))
        return {
            "task": "raw_markets_snapshot",
            "mode": "local_snapshot",
            "scope_name": POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
            "skipped_external_discovery": True,
        }

    run_summary, _, _, raw_delta, raw_metadata = asset_helpers._run_with_raw_snapshot(
        config.raw_snapshot_level,
        _local_snapshot,
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
    )
    context.log.info(
        "DuckDB delta after polymarket_us_midterms_2026_raw_markets_snapshot: %s",
        raw_delta,
    )
    context.log.info("Run summary for raw markets local snapshot: %s", run_summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text("gamma-api.polymarket.com"),
            **raw_metadata,
        }
    )


@multi_asset(
    name="polymarket_us_midterms_2026_ops_market_scope_registry",
    specs=[
        AssetSpec(
            key=POLYMARKET_US_MIDTERMS_2026_OPS_MARKET_SCOPE_REGISTRY,
            deps=[POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS_SNAPSHOT],
        )
    ],
    group_name="ingestion",
)
def polymarket_us_midterms_2026_ops_market_scope_registry(
    context: AssetExecutionContext,
    config: MarketScopeRegistryConfig,
) -> MaterializeResult:
    def _registry_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)

    if config.skip_if_snapshot_refreshed and not config.force_refresh:
        snapshot_metrics = get_sync_run_metrics("sync_markets")
        refreshed_scope_name = (
            _snapshot_refreshed_scope_name(snapshot_metrics)
            if snapshot_metrics
            else None
        )
        if (
            snapshot_metrics
            and snapshot_metrics.get("registry_refreshed") is True
            and refreshed_scope_name == POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME
        ):
            context.log.info(
                "Skipping market-scope registry refresh; snapshot already refreshed registry"
            )
            pre = snapshot_raw_layer(level=config.raw_snapshot_level)
            run_summary = {
                "skipped": True,
                "reason": "snapshot_refreshed_registry",
                "scope_name": POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
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

    def _sync_registry(_pre: dict[str, Any]) -> dict[str, Any]:
        return ops.sync_market_scope_registry(
            scope_name=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
            max_event_pages=config.max_event_pages,
            max_pages_without_progress=config.max_pages_without_progress,
            keyset_closed=config.keyset_closed,
            keyset_tag_slugs=config.keyset_tag_slugs,
            keyset_volume_min=config.keyset_volume_min,
            progress_callback=_registry_progress,
        )

    run_summary, _, _, _, raw_metadata = asset_helpers._run_with_raw_snapshot(
        config.raw_snapshot_level,
        _sync_registry,
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
    )
    return MaterializeResult(metadata=raw_metadata)


@multi_asset(
    name="polymarket_us_midterms_2026_raw_market_metadata_backfill",
    specs=[
        AssetSpec(
            key=POLYMARKET_US_MIDTERMS_2026_RAW_MARKET_METADATA_BACKFILL,
            deps=[POLYMARKET_US_MIDTERMS_2026_OPS_MARKET_SCOPE_REGISTRY],
        )
    ],
    group_name="ingestion",
)
def polymarket_us_midterms_2026_raw_market_metadata_backfill(
    context: AssetExecutionContext,
    config: MetadataBackfillConfig,
) -> MaterializeResult:
    guardrail = ops.ProgressGuardrail(
        asset="polymarket_us_midterms_2026_raw_market_metadata_backfill",
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
        asset_helpers._run_with_guardrail_thread(
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
                market_scope=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
                event_slug_fallback_max_pages=config.event_slug_fallback_max_pages,
                event_slug_fallback_max_pages_without_progress=config.event_slug_fallback_max_pages_without_progress,
                event_slug_fallback_progress_every_pages=config.event_slug_fallback_progress_pages,
            ),
            poll_seconds=config.progress_poll_seconds,
            thread_factory=ops.Thread,
        )
    ]
    orphan_market_tokens_removed = ops.delete_orphan_market_tokens(
        scope_name=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME
    )
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
            **asset_helpers._raw_snapshot_metadata(pre, post, dd),
            "backfill_summaries": MetadataValue.json(backfill_summaries),
            "orphan_market_tokens_removed": MetadataValue.int(
                orphan_market_tokens_removed
            ),
        }
    )


@multi_asset(
    name="polymarket_us_midterms_2026_raw_token_odds_history_hourly",
    specs=[
        AssetSpec(
            key=POLYMARKET_US_MIDTERMS_2026_RAW_TOKEN_ODDS_HISTORY_HOURLY,
            deps=[POLYMARKET_US_MIDTERMS_2026_RAW_MARKET_METADATA_BACKFILL],
        )
    ],
    group_name="ingestion",
)
def polymarket_us_midterms_2026_raw_token_odds_history_hourly(
    context: AssetExecutionContext,
    config: HourlyOddsSyncConfig,
) -> MaterializeResult:
    def _run_with_raw_snapshot(raw_snapshot_level, run_fn):
        return asset_helpers._run_with_raw_snapshot(
            raw_snapshot_level,
            run_fn,
            snapshot_raw_layer_fn=snapshot_raw_layer,
            delta_raw_layer_fn=delta_raw_layer,
        )

    return asset_helpers._materialize_odds_sync(
        context,
        config,
        market_scope=POLYMARKET_US_MIDTERMS_2026_SCOPE_NAME,
        sync_odds_fn=ops.sync_odds,
        run_with_raw_snapshot_fn=_run_with_raw_snapshot,
    )


__all__ = [
    "POLYMARKET_US_MIDTERMS_2026_OPS_MARKET_SCOPE_REGISTRY",
    "POLYMARKET_US_MIDTERMS_2026_RAW_MARKET_METADATA_BACKFILL",
    "POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS",
    "POLYMARKET_US_MIDTERMS_2026_RAW_MARKETS_SNAPSHOT",
    "POLYMARKET_US_MIDTERMS_2026_RAW_TOKEN_ODDS_HISTORY_HOURLY",
    "polymarket_us_midterms_2026_raw_market_metadata_backfill",
    "polymarket_us_midterms_2026_raw_markets",
    "polymarket_us_midterms_2026_raw_markets_snapshot",
    "polymarket_us_midterms_2026_raw_token_odds_history_hourly",
    "polymarket_us_midterms_2026_ops_market_scope_registry",
]
