from dagster import AssetSelection, define_asset_job, multiprocess_executor

from oddsfox_pipeline.orchestration.config import (
    wc2026_dbt_build_run_config,
    wc2026_full_refresh_events_run_config,
    wc2026_hourly_odds_run_config,
)

_ANALYTICS_BUILD_EXECUTOR = multiprocess_executor.configured(
    {"max_concurrent": 1},
    name="duckdb_serial_multiprocess",
)
_DUCKDB_WAREHOUSE_TAGS = {"duckdb_warehouse": "true"}


def _merge_run_configs(*configs: dict) -> dict:
    merged: dict = {"ops": {}}
    for config in configs:
        merged["ops"].update(config.get("ops", {}))
    return merged


WC2026_MARKET_REGISTRY_SELECTION = AssetSelection.assets(
    "wc2026_polymarket_raw_markets",
    "wc2026_polymarket_markets_snapshot",
    "wc2026_polymarket_market_registry",
    "wc2026_polymarket_market_metadata_backfill",
)

WC2026_HOURLY_ODDS_SELECTION = AssetSelection.assets(
    "wc2026_polymarket_token_odds_history_hourly",
)

WC2026_FULL_PIPELINE_SELECTION = (
    WC2026_MARKET_REGISTRY_SELECTION
    | WC2026_HOURLY_ODDS_SELECTION
    | AssetSelection.groups("analytics")
)

wc2026_market_registry_refresh = define_asset_job(
    "wc2026_market_registry_refresh",
    selection=WC2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=wc2026_full_refresh_events_run_config(),
    tags={**_DUCKDB_WAREHOUSE_TAGS, "scope": "wc2026"},
)

wc2026_hourly_odds_ingest = define_asset_job(
    "wc2026_hourly_odds_ingest",
    selection=WC2026_HOURLY_ODDS_SELECTION,
    config=wc2026_hourly_odds_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags={**_DUCKDB_WAREHOUSE_TAGS, "scope": "wc2026"},
)

wc2026_dbt_build = define_asset_job(
    "wc2026_dbt_build",
    selection=AssetSelection.groups("analytics"),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=wc2026_dbt_build_run_config(),
    tags={**_DUCKDB_WAREHOUSE_TAGS, "scope": "wc2026"},
)

wc2026_full_pipeline = define_asset_job(
    "wc2026_full_pipeline",
    selection=WC2026_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=_merge_run_configs(
        wc2026_full_refresh_events_run_config(),
        wc2026_hourly_odds_run_config(),
        wc2026_dbt_build_run_config(),
    ),
    tags={**_DUCKDB_WAREHOUSE_TAGS, "scope": "wc2026"},
)
