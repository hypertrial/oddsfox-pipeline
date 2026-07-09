from dagster import AssetSelection, define_asset_job, multiprocess_executor
from dagster_dbt import build_dbt_asset_selection

from oddsfox_pipeline.naming import (
    SCOPE_US_MIDTERMS_2026,
    SCOPE_WC2026,
    SOURCE_INTERNATIONAL_RESULTS,
    SOURCE_POLYMARKET,
    asset_key,
)
from oddsfox_pipeline.orchestration.assets_polymarket import polymarket_wc2026_dbt
from oddsfox_pipeline.orchestration.config import (
    polymarket_us_midterms_2026_full_refresh_events_run_config,
    polymarket_us_midterms_2026_hourly_odds_run_config,
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
_POLYMARKET_US_MIDTERMS_2026_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_POLYMARKET,
    "scope": SCOPE_US_MIDTERMS_2026,
}
_INTERNATIONAL_RESULTS_WC2026_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_INTERNATIONAL_RESULTS,
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

INTERNATIONAL_RESULTS_WC2026_MATCH_RESULTS_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "raw", "match_results"),
)

POLYMARKET_WC2026_FULL_PIPELINE_SELECTION = (
    INTERNATIONAL_RESULTS_WC2026_MATCH_RESULTS_SELECTION
    | POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_WC2026_HOURLY_ODDS_SELECTION
    | AssetSelection.groups("analytics")
)

POLYMARKET_US_MIDTERMS_2026_MARKET_REGISTRY_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "markets"),
    asset_key(SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "markets_snapshot"),
    asset_key(
        SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "ops", "market_scope_registry"
    ),
    asset_key(
        SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "market_metadata_backfill"
    ),
)

POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SELECTION = AssetSelection.assets(
    asset_key(
        SOURCE_POLYMARKET, SCOPE_US_MIDTERMS_2026, "raw", "token_odds_history_hourly"
    ),
)

POLYMARKET_US_MIDTERMS_2026_DBT_SELECTION = build_dbt_asset_selection(
    [polymarket_wc2026_dbt],
    dbt_select="tag:us_midterms_2026",
)

POLYMARKET_US_MIDTERMS_2026_FULL_PIPELINE_SELECTION = (
    POLYMARKET_US_MIDTERMS_2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SELECTION
    | POLYMARKET_US_MIDTERMS_2026_DBT_SELECTION
)

polymarket_us_midterms_2026_market_registry_refresh = define_asset_job(
    "polymarket_us_midterms_2026_market_registry_refresh",
    selection=POLYMARKET_US_MIDTERMS_2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_us_midterms_2026_full_refresh_events_run_config(),
    tags=_POLYMARKET_US_MIDTERMS_2026_TAGS,
)

polymarket_us_midterms_2026_hourly_odds_ingest = define_asset_job(
    "polymarket_us_midterms_2026_hourly_odds_ingest",
    selection=POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SELECTION,
    config=polymarket_us_midterms_2026_hourly_odds_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_POLYMARKET_US_MIDTERMS_2026_TAGS,
)

polymarket_us_midterms_2026_full_pipeline = define_asset_job(
    "polymarket_us_midterms_2026_full_pipeline",
    selection=POLYMARKET_US_MIDTERMS_2026_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=_merge_run_configs(
        polymarket_us_midterms_2026_full_refresh_events_run_config(),
        polymarket_us_midterms_2026_hourly_odds_run_config(),
        polymarket_wc2026_dbt_build_run_config(),
    ),
    tags=_POLYMARKET_US_MIDTERMS_2026_TAGS,
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

international_results_wc2026_match_results_ingest = define_asset_job(
    "international_results_wc2026_match_results_ingest",
    selection=INTERNATIONAL_RESULTS_WC2026_MATCH_RESULTS_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_INTERNATIONAL_RESULTS_WC2026_TAGS,
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
