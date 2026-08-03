import dlt
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    multi_asset,
)
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets

from oddsfox_pipeline.config.settings import DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE
from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    normalize_market_payloads_for_dlt,
    polymarket_wc2026_markets_source,
)
from oddsfox_pipeline.ingestion.polymarket.event_catalog import (
    collect_wc2026_event_catalog,
)
from oddsfox_pipeline.ingestion.polymarket.markets.sync import (
    collect_market_scope_payload,
)
from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration import polymarket_asset_helpers as asset_helpers
from oddsfox_pipeline.orchestration import polymarket_ops as ops
from oddsfox_pipeline.orchestration.assets_openfootball import (
    OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES,
)
from oddsfox_pipeline.orchestration.config import (
    DbtBuildConfig,
    HourlyOddsSyncConfig,
    MarketScopeRegistryConfig,
    MarketsSyncConfig,
    MatchMinuteOddsSyncConfig,
    MetadataEnrichmentConfig,
)
from oddsfox_pipeline.orchestration.dbt_project import DBT_PROJECT
from oddsfox_pipeline.orchestration.snapshot_helpers import (
    _snapshot_refreshed_scope_name,
)
from oddsfox_pipeline.orchestration.translators import PolymarketDagsterDbtTranslator
from oddsfox_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    get_connection,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch_event_catalog import (
    merge_event_catalog_batch,
)
from oddsfox_pipeline.storage.duckdb.markets import save_market_tokens_batch
from oddsfox_pipeline.storage.duckdb.metadata import (
    get_sync_run_metrics,
    save_sync_run_metrics,
)
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import ensure_polymarket_indexes

POLYMARKET_WC2026_SCOPE_NAME = DEFAULT_POLYMARKET_WC2026_MARKET_SCOPE
POLYMARKET_WC2026_RAW_MARKETS = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "markets"
)
POLYMARKET_WC2026_RAW_MARKETS_SNAPSHOT = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "markets_snapshot"
)
POLYMARKET_WC2026_RAW_EVENT_CATALOG = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "event_catalog"
)
POLYMARKET_WC2026_RAW_EVENT_SNAPSHOTS = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "event_snapshots"
)
POLYMARKET_WC2026_RAW_EVENT_MARKET_MEMBERSHIPS = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "event_market_memberships"
)
POLYMARKET_WC2026_OPS_MARKET_SCOPE_REGISTRY = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "ops", "market_scope_registry"
)
POLYMARKET_WC2026_RAW_MARKET_METADATA_BACKFILL = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "market_metadata_enrichment"
)
POLYMARKET_WC2026_RAW_TOKEN_ODDS_HISTORY_HOURLY = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "token_odds_history_hourly"
)
POLYMARKET_WC2026_RAW_MATCH_TOKEN_ODDS_HISTORY_MINUTE = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "match_token_odds_history_minute"
)


class PolymarketWc2026DltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data):
        spec = super().get_asset_spec(data)
        resource = data.resource
        if resource.source_name == "polymarket_wc2026" and resource.name == "markets":
            return spec.replace_attributes(
                key=POLYMARKET_WC2026_RAW_MARKETS,
                deps=[OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES],
            )
        return (
            spec  # pragma: no cover - current WC2026 dlt source exposes only markets.
        )


_POLYMARKET_DLT_PIPELINE = asset_helpers.get_polymarket_dlt_pipeline(
    active_duckdb_path_fn=active_duckdb_path,
    dlt_module=dlt,
)


@dlt_assets(
    name="polymarket_wc2026_raw_markets",
    group_name="ingestion",
    dlt_source=polymarket_wc2026_markets_source(),
    dlt_pipeline=_POLYMARKET_DLT_PIPELINE,
    dagster_dlt_translator=PolymarketWc2026DltTranslator(),
)
def polymarket_wc2026_raw_markets(
    context: AssetExecutionContext,
    config: MarketsSyncConfig,
    dlt: DagsterDltResource,
):
    yield from asset_helpers._run_raw_markets(
        context,
        config,
        dlt,
        asset_name="polymarket_wc2026_raw_markets",
        scope_name=POLYMARKET_WC2026_SCOPE_NAME,
        discovery_mode=config.discovery_mode,
        source_fn=polymarket_wc2026_markets_source,
        collect_market_scope_payload_fn=collect_market_scope_payload,
        save_market_tokens_batch_fn=save_market_tokens_batch,
        save_sync_run_metrics_fn=save_sync_run_metrics,
        get_connection_fn=get_connection,
        ensure_indexes_fn=ensure_polymarket_indexes,
        active_duckdb_path_fn=active_duckdb_path,
    )


@multi_asset(
    name="polymarket_wc2026_raw_markets_snapshot",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_MARKETS_SNAPSHOT,
            deps=[POLYMARKET_WC2026_RAW_MARKETS],
        )
    ],
    group_name="ingestion",
)
def polymarket_wc2026_raw_markets_snapshot(
    context: AssetExecutionContext,
    config: MarketsSyncConfig,
) -> MaterializeResult:
    return asset_helpers._materialize_raw_markets_snapshot(
        context,
        config,
        asset_name="polymarket_wc2026_raw_markets_snapshot",
        scope_name=POLYMARKET_WC2026_SCOPE_NAME,
        source="gamma-api.polymarket.com",
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
        format_raw_snapshot_log_fn=format_raw_snapshot_log,
    )


@multi_asset(
    name="polymarket_wc2026_raw_event_catalog",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_EVENT_CATALOG,
            deps=[OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES],
        ),
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_EVENT_SNAPSHOTS,
            deps=[OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES],
        ),
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_EVENT_MARKET_MEMBERSHIPS,
            deps=[OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES],
        ),
    ],
    group_name="ingestion",
)
def polymarket_wc2026_raw_event_catalog(
    context: AssetExecutionContext,
    config: MarketScopeRegistryConfig,
):
    """Persist converged event snapshots and every child market independently."""
    batch = collect_wc2026_event_catalog(max_pages=config.max_event_pages)
    market_rows = normalize_market_payloads_for_dlt(
        batch.market_payloads,
        observed_at=batch.summary["observed_at"],
    )
    with get_connection() as conn:
        merge_event_catalog_batch(
            event_rows=batch.event_snapshots,
            tag_rows=batch.event_tag_snapshots,
            event_market_rows=batch.event_market_snapshots,
            market_rows=market_rows,
            conn=conn,
        )
        ensure_polymarket_indexes(conn, scope_name=POLYMARKET_WC2026_SCOPE_NAME)

    save_sync_run_metrics(
        "event_catalog", batch.summary, scope_name=POLYMARKET_WC2026_SCOPE_NAME
    )
    context.log.info("WC2026 event catalog: %s", batch.summary)
    yield MaterializeResult(
        asset_key=POLYMARKET_WC2026_RAW_EVENT_SNAPSHOTS,
        metadata={
            "events": len(batch.event_snapshots),
            "event_tags": len(batch.event_tag_snapshots),
            "observed_at": batch.summary["observed_at"],
        },
    )
    yield MaterializeResult(
        asset_key=POLYMARKET_WC2026_RAW_EVENT_MARKET_MEMBERSHIPS,
        metadata={
            "event_markets": len(batch.event_market_snapshots),
            "unique_markets": len(batch.market_payloads),
            "observed_at": batch.summary["observed_at"],
        },
    )
    yield MaterializeResult(
        asset_key=POLYMARKET_WC2026_RAW_EVENT_CATALOG,
        metadata=batch.summary,
    )


@multi_asset(
    name="polymarket_wc2026_ops_market_scope_registry",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_OPS_MARKET_SCOPE_REGISTRY,
            deps=[POLYMARKET_WC2026_RAW_EVENT_CATALOG],
        )
    ],
    group_name="ingestion",
)
def polymarket_wc2026_ops_market_scope_registry(
    context: AssetExecutionContext,
    config: MarketScopeRegistryConfig,
) -> MaterializeResult:
    return asset_helpers._materialize_market_scope_registry(
        context,
        config,
        scope_name=POLYMARKET_WC2026_SCOPE_NAME,
        get_sync_run_metrics_fn=get_sync_run_metrics,
        snapshot_refreshed_scope_name_fn=_snapshot_refreshed_scope_name,
        sync_market_scope_registry_fn=ops.sync_market_scope_registry,
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
    )


@multi_asset(
    name="polymarket_wc2026_raw_market_metadata_enrichment",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_MARKET_METADATA_BACKFILL,
            deps=[POLYMARKET_WC2026_OPS_MARKET_SCOPE_REGISTRY],
        )
    ],
    group_name="ingestion",
)
def polymarket_wc2026_raw_market_metadata_enrichment(
    context: AssetExecutionContext,
    config: MetadataEnrichmentConfig,
) -> MaterializeResult:
    return asset_helpers._materialize_metadata_enrichment(
        context,
        config,
        asset_name="polymarket_wc2026_raw_market_metadata_enrichment",
        scope_name=POLYMARKET_WC2026_SCOPE_NAME,
        enrich_market_metadata_fn=ops.enrich_market_metadata,
        delete_orphan_market_tokens_fn=ops.delete_orphan_market_tokens,
        snapshot_raw_layer_fn=snapshot_raw_layer,
        delta_raw_layer_fn=delta_raw_layer,
    )


@multi_asset(
    name="polymarket_wc2026_raw_token_odds_history_hourly",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_TOKEN_ODDS_HISTORY_HOURLY,
            deps=[POLYMARKET_WC2026_RAW_MARKET_METADATA_BACKFILL],
        )
    ],
    group_name="ingestion",
)
def polymarket_wc2026_raw_token_odds_history_hourly(
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
        sync_odds_fn=ops.sync_odds,
        run_with_raw_snapshot_fn=_run_with_raw_snapshot,
    )


@multi_asset(
    name="polymarket_wc2026_raw_match_token_odds_history_minute",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_MATCH_TOKEN_ODDS_HISTORY_MINUTE,
            deps=[POLYMARKET_WC2026_RAW_MARKET_METADATA_BACKFILL],
        )
    ],
    group_name="ingestion",
)
def polymarket_wc2026_raw_match_token_odds_history_minute(
    context: AssetExecutionContext,
    config: MatchMinuteOddsSyncConfig,
) -> MaterializeResult:
    try:
        with get_connection() as conn:
            summary = ops.sync_match_minute_odds_history(
                conn,
                log=context.log,
                workers=config.workers,
                requests_per_second=config.requests_per_second,
                transient_retries=config.transient_retries,
                transient_backoff_seconds=config.transient_backoff_seconds,
                progress_log_interval_seconds=config.progress_log_interval_seconds,
                no_progress_soft_timeout_seconds=(
                    config.no_progress_soft_timeout_seconds
                ),
                no_progress_hard_timeout_seconds=(
                    config.no_progress_hard_timeout_seconds
                ),
            )
    except Exception as exc:
        failure = dict(getattr(exc, "summary", {}))
        failure.setdefault("status", "preflight_error")
        failure.setdefault("error_type", exc.__class__.__name__)
        save_sync_run_metrics(
            "match_minute_odds",
            failure,
            scope_name=POLYMARKET_WC2026_SCOPE_NAME,
        )
        raise
    save_sync_run_metrics(
        "match_minute_odds",
        summary,
        scope_name=POLYMARKET_WC2026_SCOPE_NAME,
    )
    return MaterializeResult(metadata=summary)


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="oddsfox_dbt",
    dagster_dbt_translator=PolymarketDagsterDbtTranslator(),
)
def oddsfox_dbt(
    context: AssetExecutionContext, dbt: DbtCliResource, config: DbtBuildConfig
):
    pre_raw = snapshot_raw_layer(level=config.raw_snapshot_level)
    pre_dbt = snapshot_dbt_models(
        dbt_select=config.dbt_select,
        dbt_exclude=config.dbt_exclude,
    )

    yield from ops.stream_dbt_build(
        asset_name="oddsfox_dbt",
        context=context,
        dbt=dbt,
        config=config,
    )

    post_raw = snapshot_raw_layer(level=config.raw_snapshot_level)
    post_dbt = snapshot_dbt_models(
        dbt_select=config.dbt_select,
        dbt_exclude=config.dbt_exclude,
    )
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
    "POLYMARKET_WC2026_OPS_MARKET_SCOPE_REGISTRY",
    "POLYMARKET_WC2026_RAW_MARKET_METADATA_BACKFILL",
    "POLYMARKET_WC2026_RAW_MARKETS",
    "POLYMARKET_WC2026_RAW_MARKETS_SNAPSHOT",
    "POLYMARKET_WC2026_RAW_EVENT_CATALOG",
    "POLYMARKET_WC2026_RAW_EVENT_SNAPSHOTS",
    "POLYMARKET_WC2026_RAW_EVENT_MARKET_MEMBERSHIPS",
    "POLYMARKET_WC2026_RAW_MATCH_TOKEN_ODDS_HISTORY_MINUTE",
    "POLYMARKET_WC2026_RAW_TOKEN_ODDS_HISTORY_HOURLY",
    "oddsfox_dbt",
    "polymarket_wc2026_raw_market_metadata_enrichment",
    "polymarket_wc2026_raw_markets",
    "polymarket_wc2026_raw_markets_snapshot",
    "polymarket_wc2026_raw_event_catalog",
    "polymarket_wc2026_raw_match_token_odds_history_minute",
    "polymarket_wc2026_raw_token_odds_history_hourly",
    "polymarket_wc2026_ops_market_scope_registry",
]
