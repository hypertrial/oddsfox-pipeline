from dagster import AssetSelection, define_asset_job, multiprocess_executor
from dagster_dbt import build_dbt_asset_selection

from oddsfox_pipeline.naming import (
    SCOPE_US_MIDTERMS_2026,
    SCOPE_WC2026,
    SOURCE_INTERNATIONAL_RESULTS,
    SOURCE_KALSHI,
    SOURCE_OPENFOOTBALL,
    SOURCE_POLYMARKET,
    asset_key,
)
from oddsfox_pipeline.orchestration.assets_polymarket import oddsfox_dbt
from oddsfox_pipeline.orchestration.config import (
    kalshi_wc2026_dbt_build_run_config,
    kalshi_wc2026_full_refresh_events_run_config,
    kalshi_wc2026_hourly_odds_run_config,
    polymarket_us_midterms_2026_dbt_build_run_config,
    polymarket_us_midterms_2026_full_refresh_events_run_config,
    polymarket_us_midterms_2026_hourly_odds_run_config,
    polymarket_wc2026_dbt_build_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    polymarket_wc2026_match_minute_odds_run_config,
    wc2026_knockout_match_odds_full_pipeline_run_config,
)
from oddsfox_pipeline.orchestration.scope_registry import (
    KALSHI_WC2026_SCOPE,
    POLYMARKET_US_MIDTERMS_2026_SCOPE,
    POLYMARKET_WC2026_SCOPE,
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
_KALSHI_WC2026_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_KALSHI,
    "scope": SCOPE_WC2026,
}
_INTERNATIONAL_RESULTS_WC2026_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_INTERNATIONAL_RESULTS,
    "scope": SCOPE_WC2026,
}
_WC2026_CROSS_DOMAIN_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": "cross_domain",
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

POLYMARKET_WC2026_MATCH_MINUTE_RAW_SELECTION = AssetSelection.assets(
    asset_key(
        SOURCE_POLYMARKET,
        SCOPE_WC2026,
        "raw",
        "match_token_odds_history_minute",
    ),
)

POLYMARKET_WC2026_DBT_SELECTION = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select=POLYMARKET_WC2026_SCOPE.dbt_select,
    dbt_exclude=POLYMARKET_WC2026_SCOPE.dbt_exclude,
)

_POLYMARKET_WC2026_MATCH_MINUTE_DBT_GRAPH = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select="+polymarket_wc2026_match_minute_odds",
)
# Re-attach checks only to selected assets. The dbt selector's indirect test
# expansion can otherwise include relationship tests for sibling model branches.
POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION = (
    _POLYMARKET_WC2026_MATCH_MINUTE_DBT_GRAPH.without_checks().downstream(
        depth=0,
        include_self=True,
    )
)

INTERNATIONAL_RESULTS_WC2026_MATCH_RESULTS_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "raw", "match_results"),
)
INTERNATIONAL_RESULTS_HISTORICAL_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_INTERNATIONAL_RESULTS, "historical", "raw", "snapshot"),
)

OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_OPENFOOTBALL, SCOPE_WC2026, "raw", "knockout_fixtures"),
)

POLYMARKET_WC2026_MATCH_MINUTE_SELECTION = (
    OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_SELECTION
    | POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_WC2026_MATCH_MINUTE_RAW_SELECTION
    | POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION
)

POLYMARKET_WC2026_FULL_PIPELINE_SELECTION = (
    INTERNATIONAL_RESULTS_WC2026_MATCH_RESULTS_SELECTION
    | POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_WC2026_HOURLY_ODDS_SELECTION
    | POLYMARKET_WC2026_DBT_SELECTION
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
    [oddsfox_dbt],
    dbt_select=POLYMARKET_US_MIDTERMS_2026_SCOPE.dbt_select,
    dbt_exclude=POLYMARKET_US_MIDTERMS_2026_SCOPE.dbt_exclude,
)

POLYMARKET_US_MIDTERMS_2026_FULL_PIPELINE_SELECTION = (
    POLYMARKET_US_MIDTERMS_2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SELECTION
    | POLYMARKET_US_MIDTERMS_2026_DBT_SELECTION
)

polymarket_us_midterms_2026_market_registry_refresh = define_asset_job(
    POLYMARKET_US_MIDTERMS_2026_SCOPE.registry_job_name,
    selection=POLYMARKET_US_MIDTERMS_2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_us_midterms_2026_full_refresh_events_run_config(),
    tags=_POLYMARKET_US_MIDTERMS_2026_TAGS,
)

polymarket_us_midterms_2026_hourly_odds_ingest = define_asset_job(
    POLYMARKET_US_MIDTERMS_2026_SCOPE.odds_job_name,
    selection=POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SELECTION,
    config=polymarket_us_midterms_2026_hourly_odds_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_POLYMARKET_US_MIDTERMS_2026_TAGS,
)

polymarket_us_midterms_2026_full_pipeline = define_asset_job(
    POLYMARKET_US_MIDTERMS_2026_SCOPE.full_job_name,
    selection=POLYMARKET_US_MIDTERMS_2026_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=_merge_run_configs(
        polymarket_us_midterms_2026_full_refresh_events_run_config(),
        polymarket_us_midterms_2026_hourly_odds_run_config(),
        polymarket_us_midterms_2026_dbt_build_run_config(),
    ),
    tags=_POLYMARKET_US_MIDTERMS_2026_TAGS,
)

polymarket_wc2026_market_registry_refresh = define_asset_job(
    POLYMARKET_WC2026_SCOPE.registry_job_name,
    selection=POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_full_refresh_events_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_hourly_odds_ingest = define_asset_job(
    POLYMARKET_WC2026_SCOPE.odds_job_name,
    selection=POLYMARKET_WC2026_HOURLY_ODDS_SELECTION,
    config=polymarket_wc2026_hourly_odds_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_dbt_build = define_asset_job(
    POLYMARKET_WC2026_SCOPE.dbt_job_name,
    selection=POLYMARKET_WC2026_DBT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_dbt_build_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_match_minute_odds_backfill = define_asset_job(
    "polymarket_wc2026_match_minute_odds_backfill",
    selection=POLYMARKET_WC2026_MATCH_MINUTE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_match_minute_odds_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_us_midterms_2026_dbt_build = define_asset_job(
    POLYMARKET_US_MIDTERMS_2026_SCOPE.dbt_job_name,
    selection=POLYMARKET_US_MIDTERMS_2026_DBT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_us_midterms_2026_dbt_build_run_config(),
    tags=_POLYMARKET_US_MIDTERMS_2026_TAGS,
)

international_results_wc2026_match_results_ingest = define_asset_job(
    "international_results_wc2026_match_results_ingest",
    selection=INTERNATIONAL_RESULTS_WC2026_MATCH_RESULTS_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_INTERNATIONAL_RESULTS_WC2026_TAGS,
)

international_results_historical_ingest = define_asset_job(
    "international_results_historical_ingest",
    selection=INTERNATIONAL_RESULTS_HISTORICAL_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags={
        **_DUCKDB_WAREHOUSE_TAGS,
        "source": SOURCE_INTERNATIONAL_RESULTS,
        "scope": "historical",
    },
)

polymarket_wc2026_full_pipeline = define_asset_job(
    POLYMARKET_WC2026_SCOPE.full_job_name,
    selection=POLYMARKET_WC2026_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=_merge_run_configs(
        polymarket_wc2026_full_refresh_events_run_config(),
        polymarket_wc2026_hourly_odds_run_config(),
        polymarket_wc2026_dbt_build_run_config(),
    ),
    tags=_POLYMARKET_WC2026_TAGS,
)

KALSHI_WC2026_MARKET_REGISTRY_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_KALSHI, SCOPE_WC2026, "raw", "events"),
    asset_key(SOURCE_KALSHI, SCOPE_WC2026, "raw", "markets"),
    asset_key(SOURCE_KALSHI, SCOPE_WC2026, "raw", "markets_snapshot"),
    asset_key(SOURCE_KALSHI, SCOPE_WC2026, "ops", "market_scope_registry"),
)

KALSHI_WC2026_HOURLY_ODDS_SELECTION = AssetSelection.assets(
    asset_key(SOURCE_KALSHI, SCOPE_WC2026, "raw", "market_candlesticks_hourly"),
)

KALSHI_WC2026_DBT_SELECTION = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select=KALSHI_WC2026_SCOPE.dbt_select,
    dbt_exclude=KALSHI_WC2026_SCOPE.dbt_exclude,
)

KALSHI_WC2026_FULL_PIPELINE_SELECTION = (
    INTERNATIONAL_RESULTS_WC2026_MATCH_RESULTS_SELECTION
    | KALSHI_WC2026_MARKET_REGISTRY_SELECTION
    | KALSHI_WC2026_HOURLY_ODDS_SELECTION
    | KALSHI_WC2026_DBT_SELECTION
)

WC2026_KNOCKOUT_MATCH_ODDS_DBT_SELECTION = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select="+tag:cross_domain",
).without_checks()

WC2026_KNOCKOUT_MATCH_ODDS_FULL_PIPELINE_SELECTION = (
    OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_SELECTION
    | POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_WC2026_HOURLY_ODDS_SELECTION
    | KALSHI_WC2026_MARKET_REGISTRY_SELECTION
    | KALSHI_WC2026_HOURLY_ODDS_SELECTION
    | WC2026_KNOCKOUT_MATCH_ODDS_DBT_SELECTION
)

kalshi_wc2026_market_registry_refresh = define_asset_job(
    KALSHI_WC2026_SCOPE.registry_job_name,
    selection=KALSHI_WC2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=kalshi_wc2026_full_refresh_events_run_config(),
    tags=_KALSHI_WC2026_TAGS,
)

kalshi_wc2026_hourly_odds_ingest = define_asset_job(
    KALSHI_WC2026_SCOPE.odds_job_name,
    selection=KALSHI_WC2026_HOURLY_ODDS_SELECTION,
    config=kalshi_wc2026_hourly_odds_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_KALSHI_WC2026_TAGS,
)

kalshi_wc2026_dbt_build = define_asset_job(
    KALSHI_WC2026_SCOPE.dbt_job_name,
    selection=KALSHI_WC2026_DBT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=kalshi_wc2026_dbt_build_run_config(),
    tags=_KALSHI_WC2026_TAGS,
)

kalshi_wc2026_full_pipeline = define_asset_job(
    KALSHI_WC2026_SCOPE.full_job_name,
    selection=KALSHI_WC2026_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=_merge_run_configs(
        kalshi_wc2026_full_refresh_events_run_config(),
        kalshi_wc2026_hourly_odds_run_config(),
        kalshi_wc2026_dbt_build_run_config(),
    ),
    tags=_KALSHI_WC2026_TAGS,
)

wc2026_knockout_match_odds_full_pipeline = define_asset_job(
    "wc2026_knockout_match_odds_full_pipeline",
    selection=WC2026_KNOCKOUT_MATCH_ODDS_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=wc2026_knockout_match_odds_full_pipeline_run_config(),
    tags=_WC2026_CROSS_DOMAIN_TAGS,
)
