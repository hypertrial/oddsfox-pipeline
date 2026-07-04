from dagster import AssetSelection, define_asset_job, multiprocess_executor

from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration.config import (
    polymarket_wc2026_dbt_build_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
)

_ANALYTICS_BUILD_EXECUTOR = multiprocess_executor.configured(
    {"max_concurrent": 1},
    name="duckdb_serial_multiprocess",
)
_DUCKDB_WAREHOUSE_TAGS = {"duckdb_warehouse": "true"}
_POLYMARKET_WC2026_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_POLYMARKET,
    "scope": SCOPE_WC2026,
}


def _merge_run_configs(*configs: dict) -> dict:
    merged: dict = {"ops": {}}
    for config in configs:
        merged["ops"].update(config.get("ops", {}))
    return merged


POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "markets"),
    asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "markets_snapshot"),
    asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "ops", "market_scope_registry"),
    asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "market_metadata_backfill"),
)

POLYMARKET_WC2026_HOURLY_ODDS_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "token_odds_history_hourly"),
)

POLYMARKET_WC2026_FULL_PIPELINE_SELECTION = (
    POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_WC2026_HOURLY_ODDS_SELECTION
    | AssetSelection.groups("analytics")
)

polymarket_wc2026_market_registry_refresh = define_asset_job(
    "polymarket_wc2026_market_registry_refresh",
    selection=POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_full_refresh_events_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_hourly_odds_ingest = define_asset_job(
    "polymarket_wc2026_hourly_odds_ingest",
    selection=POLYMARKET_WC2026_HOURLY_ODDS_SELECTION,
    config=polymarket_wc2026_hourly_odds_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_dbt_build = define_asset_job(
    "polymarket_wc2026_dbt_build",
    selection=AssetSelection.groups("analytics"),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_dbt_build_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_full_pipeline = define_asset_job(
    "polymarket_wc2026_full_pipeline",
    selection=POLYMARKET_WC2026_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=_merge_run_configs(
        polymarket_wc2026_full_refresh_events_run_config(),
        polymarket_wc2026_hourly_odds_run_config(),
        polymarket_wc2026_dbt_build_run_config(),
    ),
    tags=_POLYMARKET_WC2026_TAGS,
)
