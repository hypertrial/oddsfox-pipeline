"""Dagster asset for PMXT trades paired with a published portrait book scan."""

from pathlib import Path

from dagster import AssetExecutionContext, AssetSpec, MaterializeResult, multi_asset

from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    default_order_book_targets_path,
)
from oddsfox_pipeline.ingestion.polymarket.match_trades import sync_match_trades
from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration.assets_match_order_book import (
    POLYMARKET_WC2026_RAW_MATCH_ORDER_BOOK_SNAPSHOTS,
)
from oddsfox_pipeline.orchestration.config import MatchOrderBookBackfillConfig
from oddsfox_pipeline.storage.duckdb.connection import get_connection

POLYMARKET_WC2026_RAW_MATCH_TRADES = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "match_trades"
)


@multi_asset(
    name="polymarket_wc2026_raw_match_trades",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_MATCH_TRADES,
            deps=[POLYMARKET_WC2026_RAW_MATCH_ORDER_BOOK_SNAPSHOTS],
        )
    ],
    group_name="ingestion",
)
def polymarket_wc2026_raw_match_trades(
    context: AssetExecutionContext,
    config: MatchOrderBookBackfillConfig,
) -> MaterializeResult:
    manifest = (
        Path(config.manifest_path)
        if config.manifest_path
        else default_order_book_targets_path()
    )
    with get_connection() as connection:
        summary = sync_match_trades(
            connection,
            manifest_path=manifest,
            requests_per_minute=config.requests_per_minute,
            monthly_credit_budget=config.monthly_credit_budget,
            transient_retries=config.transient_retries,
            transient_backoff_seconds=config.transient_backoff_seconds,
        )
    return MaterializeResult(metadata=summary)


__all__ = [
    "POLYMARKET_WC2026_RAW_MATCH_TRADES",
    "polymarket_wc2026_raw_match_trades",
]
