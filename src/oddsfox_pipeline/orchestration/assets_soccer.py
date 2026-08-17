"""Dagster assets for the Polymarket soccer match-minute scope."""

from dagster import AssetExecutionContext, AssetSpec, MaterializeResult, multi_asset

from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    normalize_market_payloads_for_dlt,
)
from oddsfox_pipeline.ingestion.polymarket.event_catalog import (
    collect_soccer_event_catalog,
)
from oddsfox_pipeline.ingestion.polymarket.soccer_match import (
    refresh_soccer_match_result_registry,
    sync_soccer_match_minute_odds_history,
)
from oddsfox_pipeline.naming import SCOPE_SOCCER, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration import polymarket_asset_helpers as asset_helpers
from oddsfox_pipeline.orchestration.config import (
    MarketScopeRegistryConfig,
    SoccerMatchMinuteOddsSyncConfig,
)
from oddsfox_pipeline.storage.duckdb.connection import get_connection
from oddsfox_pipeline.storage.duckdb.dlt_batch_event_catalog import (
    merge_event_catalog_batch,
)
from oddsfox_pipeline.storage.duckdb.metadata import save_sync_run_metrics
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import ensure_polymarket_indexes

POLYMARKET_SOCCER_RAW_EVENT_CATALOG = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "raw", "event_catalog"
)
POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "raw", "event_snapshots"
)
POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "raw", "event_market_memberships"
)
POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "ops", "match_result_registry"
)
POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE = asset_key(
    SOURCE_POLYMARKET,
    SCOPE_SOCCER,
    "raw",
    "match_result_token_odds_history_minute",
)


@multi_asset(
    name="polymarket_soccer_raw_event_catalog",
    specs=[
        AssetSpec(key=POLYMARKET_SOCCER_RAW_EVENT_CATALOG),
        AssetSpec(key=POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS),
        AssetSpec(key=POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS),
    ],
    group_name="ingestion",
)
def polymarket_soccer_raw_event_catalog(
    context: AssetExecutionContext,
    config: MarketScopeRegistryConfig,
):
    def merge_soccer_batch(**kwargs):
        return merge_event_catalog_batch(scope_name=SCOPE_SOCCER, **kwargs)

    yield from asset_helpers._materialize_event_catalog(
        context,
        config,
        asset_name="polymarket_soccer_raw_event_catalog",
        scope_name=SCOPE_SOCCER,
        collect_event_catalog_fn=collect_soccer_event_catalog,
        merge_event_catalog_batch_fn=merge_soccer_batch,
        normalize_market_payloads_fn=normalize_market_payloads_for_dlt,
        ensure_indexes_fn=ensure_polymarket_indexes,
        get_connection_fn=get_connection,
        save_sync_run_metrics_fn=save_sync_run_metrics,
        event_catalog_key=POLYMARKET_SOCCER_RAW_EVENT_CATALOG,
        event_snapshots_key=POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS,
        event_memberships_key=POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS,
    )


@multi_asset(
    name="polymarket_soccer_ops_match_result_registry",
    specs=[
        AssetSpec(
            key=POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY,
            deps=[POLYMARKET_SOCCER_RAW_EVENT_CATALOG],
        )
    ],
    group_name="ingestion",
)
def polymarket_soccer_ops_match_result_registry(
    _context: AssetExecutionContext,
) -> MaterializeResult:
    with get_connection() as conn:
        summary = refresh_soccer_match_result_registry(conn)
    save_sync_run_metrics("match_result_registry", summary, scope_name=SCOPE_SOCCER)
    return MaterializeResult(metadata=summary)


@multi_asset(
    name="polymarket_soccer_raw_match_result_token_odds_history_minute",
    specs=[
        AssetSpec(
            key=POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE,
            deps=[POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY],
        )
    ],
    group_name="ingestion",
)
def polymarket_soccer_raw_match_result_token_odds_history_minute(
    context: AssetExecutionContext,
    config: SoccerMatchMinuteOddsSyncConfig,
) -> MaterializeResult:
    summary = sync_soccer_match_minute_odds_history(
        connection_factory=get_connection,
        log=context.log,
        workers=config.workers,
        requests_per_second=config.requests_per_second,
        batch_group_size=config.batch_group_size,
        window_hours=config.window_hours,
        auto_tune_rps=config.auto_tune_rps,
        auto_tune_max_rps=config.auto_tune_max_rps,
        transient_retries=config.transient_retries,
        transient_backoff_seconds=config.transient_backoff_seconds,
        completion_grace_minutes=config.completion_grace_minutes,
        empty_retry_hours=config.empty_retry_hours,
        force=config.force,
        game_sample_size=config.game_sample_size,
    )
    save_sync_run_metrics("match_minute_odds", summary, scope_name=SCOPE_SOCCER)
    return MaterializeResult(metadata=summary)


__all__ = [
    "POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY",
    "POLYMARKET_SOCCER_RAW_EVENT_CATALOG",
    "POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS",
    "POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS",
    "POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE",
    "polymarket_soccer_ops_match_result_registry",
    "polymarket_soccer_raw_event_catalog",
    "polymarket_soccer_raw_match_result_token_odds_history_minute",
]
