"""Dagster assets for the manual global Polymarket catalog."""

from pathlib import Path

from dagster import AssetExecutionContext, AssetSpec, MaterializeResult, multi_asset

from oddsfox_pipeline.ingestion.polymarket.catalog import collect_polymarket_catalog
from oddsfox_pipeline.naming import SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration.config import (
    PolymarketCatalogReleaseConfig,
    PolymarketCatalogSyncConfig,
)
from oddsfox_pipeline.publishing.polymarket_catalog import (
    build_polymarket_catalog_release,
)
from oddsfox_pipeline.storage.duckdb.connection import get_connection

POLYMARKET_CATALOG_RAW_CRAWL = asset_key(SOURCE_POLYMARKET, "catalog", "raw", "crawl")
POLYMARKET_CATALOG_MART_GRAPH = asset_key(
    SOURCE_POLYMARKET, "catalog", "marts", "polymarket_graph_catalog"
)
POLYMARKET_CATALOG_RELEASE_GRAPH = asset_key(
    SOURCE_POLYMARKET, "catalog", "release", "polymarket_graph_catalog"
)


@multi_asset(
    name="polymarket_catalog_raw_catalog_crawl",
    specs=[AssetSpec(key=POLYMARKET_CATALOG_RAW_CRAWL)],
    group_name="ingestion",
)
def polymarket_catalog_raw_catalog_crawl(
    context: AssetExecutionContext,
    config: PolymarketCatalogSyncConfig,
) -> MaterializeResult:
    with get_connection() as conn:
        summary = collect_polymarket_catalog(
            conn,
            crawl_id=config.crawl_id,
            max_pages=config.max_pages,
        )
    context.log.info("Activated Polymarket catalog crawl %s", summary["crawl_id"])
    return MaterializeResult(metadata=summary)


@multi_asset(
    name="polymarket_catalog_release_graph_catalog",
    specs=[
        AssetSpec(
            key=POLYMARKET_CATALOG_RELEASE_GRAPH,
            deps=[POLYMARKET_CATALOG_MART_GRAPH],
        )
    ],
    group_name="release",
)
def polymarket_catalog_release_graph_catalog(
    context: AssetExecutionContext,
    config: PolymarketCatalogReleaseConfig,
) -> MaterializeResult:
    with get_connection() as conn:
        summary = build_polymarket_catalog_release(
            conn,
            Path(config.output_root),
            dataset_version=config.dataset_version,
        )
    context.log.info(
        "Published Polymarket graph catalog %s", summary["dataset_version"]
    )
    return MaterializeResult(metadata=summary)


__all__ = [
    "POLYMARKET_CATALOG_MART_GRAPH",
    "POLYMARKET_CATALOG_RAW_CRAWL",
    "POLYMARKET_CATALOG_RELEASE_GRAPH",
    "polymarket_catalog_raw_catalog_crawl",
    "polymarket_catalog_release_graph_catalog",
]
