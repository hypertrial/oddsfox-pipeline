from dagster import (
    AssetCheckKey,
    AssetSelection,
    define_asset_job,
    multiprocess_executor,
)
from dagster_dbt import build_dbt_asset_selection

from oddsfox_pipeline.config.settings import (
    POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG,
    POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES,
    POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH,
)
from oddsfox_pipeline.naming import (
    SCOPE_SOCCER,
    SCOPE_WC2026,
    SOURCE_KALSHI,
    SOURCE_POLYMARKET,
    asset_key,
)
from oddsfox_pipeline.orchestration.assets_match_order_book import (
    POLYMARKET_WC2026_RAW_MATCH_ORDER_BOOK_SNAPSHOTS,
)
from oddsfox_pipeline.orchestration.assets_match_trades import (
    POLYMARKET_WC2026_RAW_MATCH_TRADES,
)
from oddsfox_pipeline.orchestration.assets_polygon_settlement import (
    POLYMARKET_WC2026_RAW_POLYGON_SETTLEMENT_FILLS,
    POLYMARKET_WC2026_RELEASE_POLYGON_SETTLEMENT_ODDS_BUNDLE,
)
from oddsfox_pipeline.orchestration.assets_polymarket import (
    oddsfox_dbt,
    polymarket_soccer_monitoring_dbt,
)
from oddsfox_pipeline.orchestration.assets_polymarket_catalog import (
    POLYMARKET_CATALOG_RAW_CRAWL,
    POLYMARKET_CATALOG_RELEASE_GRAPH,
)
from oddsfox_pipeline.orchestration.assets_soccer import (
    POLYMARKET_SOCCER_MART_MATCH_RESULT_MINUTE,
    POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY,
    POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT,
    POLYMARKET_SOCCER_RAW_EVENT_CATALOG,
    POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE,
)
from oddsfox_pipeline.orchestration.config import (
    kalshi_wc2026_dbt_build_run_config,
    kalshi_wc2026_full_refresh_events_run_config,
    kalshi_wc2026_hourly_odds_run_config,
    polymarket_catalog_dbt_build_run_config,
    polymarket_soccer_catalog_run_config,
    polymarket_soccer_dbt_build_run_config,
    polymarket_soccer_full_pipeline_run_config,
    polymarket_soccer_minute_odds_run_config,
    polymarket_wc2026_dbt_build_run_config,
    polymarket_wc2026_event_catalog_recall_audit_run_config,
    polymarket_wc2026_full_pipeline_run_config,
    polymarket_wc2026_full_refresh_events_run_config,
    polymarket_wc2026_hourly_odds_run_config,
    polymarket_wc2026_market_portrait_run_config,
    polymarket_wc2026_match_minute_odds_run_config,
    polymarket_wc2026_match_order_book_run_config,
    polymarket_wc2026_minute_odds_run_config,
    polymarket_wc2026_minute_odds_smoke_run_config,
    polymarket_wc2026_polygon_settlement_backfill_run_config,
)
from oddsfox_pipeline.orchestration.shipped_scopes import (
    KALSHI_WC2026_SCOPE,
    POLYMARKET_SOCCER_CORE_DBT_SELECT,
    POLYMARKET_SOCCER_MONITORING_DBT_SELECT,
    POLYMARKET_SOCCER_SCOPE,
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
_POLYMARKET_SOCCER_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_POLYMARKET,
    "scope": SCOPE_SOCCER,
}
_KALSHI_WC2026_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_KALSHI,
    "scope": SCOPE_WC2026,
}

_POLYMARKET_CATALOG_TAGS = {
    **_DUCKDB_WAREHOUSE_TAGS,
    "source": SOURCE_POLYMARKET,
    "scope": "catalog",
}

POLYMARKET_CATALOG_DBT_SELECTION = build_dbt_asset_selection(
    [oddsfox_dbt], dbt_select="+polymarket_graph_catalog"
)
POLYMARKET_CATALOG_FULL_SELECTION = (
    AssetSelection.assets(POLYMARKET_CATALOG_RAW_CRAWL)
    | POLYMARKET_CATALOG_DBT_SELECTION
)

polymarket_catalog_full_pipeline = define_asset_job(
    "polymarket_catalog_full_pipeline",
    selection=POLYMARKET_CATALOG_FULL_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_catalog_dbt_build_run_config(),
    tags=_POLYMARKET_CATALOG_TAGS,
)

polymarket_catalog_dbt_build = define_asset_job(
    "polymarket_catalog_dbt_build",
    selection=POLYMARKET_CATALOG_DBT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_catalog_dbt_build_run_config(),
    tags=_POLYMARKET_CATALOG_TAGS,
)

polymarket_catalog_release = define_asset_job(
    "polymarket_catalog_release",
    selection=AssetSelection.assets(POLYMARKET_CATALOG_RELEASE_GRAPH),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_POLYMARKET_CATALOG_TAGS,
)


def _merge_dbt_build_config(existing: dict, incoming: dict) -> dict:
    merged = {**existing, **incoming}
    selects: list[str] = []
    for cfg in (existing, incoming):
        select = cfg.get("dbt_select")
        if select:
            selects.extend(str(select).split())
    if selects:
        merged["dbt_select"] = " ".join(dict.fromkeys(selects))

    excludes: list[str] = []
    for cfg in (existing, incoming):
        exclude = cfg.get("dbt_exclude")
        if exclude:
            excludes.extend(str(exclude).split())
    if excludes:
        merged["dbt_exclude"] = " ".join(dict.fromkeys(excludes))
    return merged


def _merge_op_config(existing: dict | None, incoming: dict) -> dict:
    if not existing:
        return dict(incoming)
    existing_cfg = existing.get("config")
    incoming_cfg = incoming.get("config")
    if isinstance(existing_cfg, dict) and isinstance(incoming_cfg, dict):
        if {"dbt_select", "dbt_exclude"} & (set(existing_cfg) | set(incoming_cfg)):
            merged_cfg = _merge_dbt_build_config(existing_cfg, incoming_cfg)
            return {**existing, **incoming, "config": merged_cfg}
    return {**existing, **incoming}


def _merge_run_configs(*configs: dict) -> dict:
    merged: dict = {"ops": {}}
    for config in configs:
        for op_name, op_config in config.get("ops", {}).items():
            existing = merged["ops"].get(op_name)
            merged["ops"][op_name] = _merge_op_config(existing, op_config)
    return merged


POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION = (
    AssetSelection.assets(
        asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "markets"),
        asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "markets_snapshot"),
        asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "ops", "market_scope_registry"),
        asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "market_metadata_enrichment"),
    )
    | AssetSelection.assets(
        asset_key(SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "event_catalog"),
    ).required_multi_asset_neighbors()
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

POLYMARKET_WC2026_FUTURES_MINUTE_RAW_SELECTION = AssetSelection.assets(
    asset_key(
        SOURCE_POLYMARKET,
        SCOPE_WC2026,
        "raw",
        "futures_token_odds_history_minute",
    ),
)

POLYMARKET_WC2026_MATCH_ORDER_BOOK_RAW_SELECTION = AssetSelection.assets(
    POLYMARKET_WC2026_RAW_MATCH_ORDER_BOOK_SNAPSHOTS
)
POLYMARKET_WC2026_MATCH_TRADES_RAW_SELECTION = AssetSelection.assets(
    POLYMARKET_WC2026_RAW_MATCH_TRADES
)

POLYMARKET_WC2026_POLYGON_SETTLEMENT_RAW_SELECTION = AssetSelection.assets(
    POLYMARKET_WC2026_RAW_POLYGON_SETTLEMENT_FILLS
)

POLYMARKET_WC2026_POLYGON_SETTLEMENT_RELEASE_SELECTION = AssetSelection.assets(
    POLYMARKET_WC2026_RELEASE_POLYGON_SETTLEMENT_ODDS_BUNDLE
)

_POLYMARKET_WC2026_GOLDEN_MART_DBT_GRAPH = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select=POLYMARKET_WC2026_SCOPE.dbt_select,
    dbt_exclude=POLYMARKET_WC2026_SCOPE.dbt_exclude,
)
POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECTION = (
    _POLYMARKET_WC2026_GOLDEN_MART_DBT_GRAPH.without_checks().downstream(
        depth=0,
        include_self=True,
    )
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

_POLYMARKET_WC2026_MINUTE_ODDS_DBT_GRAPH = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select="+polymarket_wc2026_market_minute_odds_data_quality",
)
POLYMARKET_WC2026_MINUTE_ODDS_DBT_SELECTION = (
    _POLYMARKET_WC2026_MINUTE_ODDS_DBT_GRAPH.without_checks().downstream(
        depth=0,
        include_self=True,
    )
)

_POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_GRAPH = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select="+tag:pmxt_order_book",
)
POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_SELECTION = (
    _POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_GRAPH.without_checks().downstream(
        depth=0,
        include_self=True,
    )
)

_POLYMARKET_WC2026_MARKET_PORTRAIT_DBT_GRAPH = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select="+tag:pmxt_order_book +tag:market_portrait",
)
POLYMARKET_WC2026_MARKET_PORTRAIT_DBT_SELECTION = (
    _POLYMARKET_WC2026_MARKET_PORTRAIT_DBT_GRAPH.without_checks().downstream(
        depth=0,
        include_self=True,
    )
)

_POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_GRAPH = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select="+polymarket_wc2026_polygon_settlement_minute_odds",
)
POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_SELECTION = (
    _POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_GRAPH.without_checks().downstream(
        depth=0,
        include_self=True,
    )
)

POLYMARKET_WC2026_POLYGON_SETTLEMENT_BACKFILL_SELECTION = (
    POLYMARKET_WC2026_POLYGON_SETTLEMENT_RAW_SELECTION
    | POLYMARKET_WC2026_POLYGON_SETTLEMENT_DBT_SELECTION
)

POLYMARKET_WC2026_MATCH_MINUTE_SELECTION = (
    POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_WC2026_MATCH_MINUTE_RAW_SELECTION
    | POLYMARKET_WC2026_MATCH_MINUTE_DBT_SELECTION
)


def build_polymarket_wc2026_minute_odds_selection() -> AssetSelection:
    """Asset selection for unified minute-odds; gated by REFRESH_* env flags."""
    selection = POLYMARKET_WC2026_MINUTE_ODDS_DBT_SELECTION
    if POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH:
        selection = POLYMARKET_WC2026_MATCH_MINUTE_RAW_SELECTION | selection
    if POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES:
        selection = POLYMARKET_WC2026_FUTURES_MINUTE_RAW_SELECTION | selection
    if POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG:
        selection = POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION | selection
    return selection


POLYMARKET_WC2026_MINUTE_ODDS_SELECTION = (
    build_polymarket_wc2026_minute_odds_selection()
)

POLYMARKET_WC2026_MATCH_ORDER_BOOK_SELECTION = (
    POLYMARKET_WC2026_MATCH_ORDER_BOOK_RAW_SELECTION
    | POLYMARKET_WC2026_MATCH_ORDER_BOOK_DBT_SELECTION
)
POLYMARKET_WC2026_MARKET_PORTRAIT_SELECTION = (
    POLYMARKET_WC2026_MATCH_ORDER_BOOK_RAW_SELECTION
    | POLYMARKET_WC2026_MATCH_TRADES_RAW_SELECTION
    | POLYMARKET_WC2026_MARKET_PORTRAIT_DBT_SELECTION
)

POLYMARKET_WC2026_FULL_PIPELINE_SELECTION = (
    POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION
    | POLYMARKET_WC2026_HOURLY_ODDS_SELECTION
    | POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECTION
)

POLYMARKET_SOCCER_PREFLIGHT_SELECTION = AssetSelection.assets(
    POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT
).required_multi_asset_neighbors().without_checks() | AssetSelection.checks(
    AssetCheckKey(
        asset_key=POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT,
        name="local_contracts_valid",
    )
)
POLYMARKET_SOCCER_CATALOG_SELECTION = (
    POLYMARKET_SOCCER_PREFLIGHT_SELECTION
    | AssetSelection.assets(POLYMARKET_SOCCER_RAW_EVENT_CATALOG)
    .required_multi_asset_neighbors()
    .without_checks()
    | AssetSelection.assets(
        POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY
    ).without_checks()
    | AssetSelection.checks(
        AssetCheckKey(
            asset_key=POLYMARKET_SOCCER_RAW_EVENT_CATALOG,
            name="catalog_converged",
        ),
        AssetCheckKey(
            asset_key=POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY,
            name="three_roles_and_six_tokens",
        ),
    )
)

POLYMARKET_SOCCER_MINUTE_RAW_SELECTION = (
    POLYMARKET_SOCCER_PREFLIGHT_SELECTION
    | AssetSelection.assets(POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE).without_checks()
    | AssetSelection.checks(
        AssetCheckKey(
            asset_key=POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE,
            name="exact_window_publication_reconciled",
        ),
        AssetCheckKey(
            asset_key=POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE,
            name="production_health",
        ),
    )
)

_POLYMARKET_SOCCER_DBT_GRAPH = build_dbt_asset_selection(
    [oddsfox_dbt],
    dbt_select=POLYMARKET_SOCCER_CORE_DBT_SELECT,
) | build_dbt_asset_selection(
    [polymarket_soccer_monitoring_dbt],
    dbt_select=POLYMARKET_SOCCER_MONITORING_DBT_SELECT,
)
POLYMARKET_SOCCER_DBT_SELECTION = (
    POLYMARKET_SOCCER_PREFLIGHT_SELECTION
    | _POLYMARKET_SOCCER_DBT_GRAPH.without_checks().downstream(
        depth=0,
        include_self=True,
    )
    | AssetSelection.checks(
        AssetCheckKey(
            asset_key=POLYMARKET_SOCCER_MART_MATCH_RESULT_MINUTE,
            name="minute_mart_contracts_valid",
        )
    )
)
POLYMARKET_SOCCER_FULL_PIPELINE_SELECTION = (
    POLYMARKET_SOCCER_CATALOG_SELECTION
    | POLYMARKET_SOCCER_MINUTE_RAW_SELECTION
    | POLYMARKET_SOCCER_DBT_SELECTION
)

polymarket_soccer_market_scope_registry_refresh = define_asset_job(
    POLYMARKET_SOCCER_SCOPE.registry_job_name,
    selection=POLYMARKET_SOCCER_CATALOG_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_soccer_catalog_run_config(),
    tags=_POLYMARKET_SOCCER_TAGS,
)

polymarket_soccer_match_result_minute_odds_ingest = define_asset_job(
    POLYMARKET_SOCCER_SCOPE.odds_job_name,
    selection=POLYMARKET_SOCCER_MINUTE_RAW_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_soccer_minute_odds_run_config(),
    tags=_POLYMARKET_SOCCER_TAGS,
)

polymarket_soccer_dbt_build = define_asset_job(
    POLYMARKET_SOCCER_SCOPE.dbt_job_name,
    selection=POLYMARKET_SOCCER_DBT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_soccer_dbt_build_run_config(),
    tags=_POLYMARKET_SOCCER_TAGS,
)

polymarket_soccer_full_pipeline = define_asset_job(
    POLYMARKET_SOCCER_SCOPE.full_job_name,
    selection=POLYMARKET_SOCCER_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_soccer_full_pipeline_run_config(),
    tags=_POLYMARKET_SOCCER_TAGS,
)

polymarket_wc2026_market_scope_registry_refresh = define_asset_job(
    POLYMARKET_WC2026_SCOPE.registry_job_name,
    selection=POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_full_refresh_events_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_event_catalog_recall_audit = define_asset_job(
    "polymarket_wc2026_event_catalog_recall_audit",
    selection=POLYMARKET_WC2026_MARKET_REGISTRY_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_event_catalog_recall_audit_run_config(),
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
    selection=POLYMARKET_WC2026_GOLDEN_MART_DBT_SELECTION,
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

polymarket_wc2026_minute_odds_backfill = define_asset_job(
    "polymarket_wc2026_minute_odds_backfill",
    selection=POLYMARKET_WC2026_MINUTE_ODDS_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_minute_odds_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_minute_odds_live_smoke = define_asset_job(
    "polymarket_wc2026_minute_odds_live_smoke",
    selection=POLYMARKET_WC2026_MINUTE_ODDS_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_minute_odds_smoke_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_match_order_book_backfill = define_asset_job(
    "polymarket_wc2026_match_order_book_backfill",
    selection=POLYMARKET_WC2026_MATCH_ORDER_BOOK_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_match_order_book_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_market_portrait_backfill = define_asset_job(
    "polymarket_wc2026_market_portrait_backfill",
    selection=POLYMARKET_WC2026_MARKET_PORTRAIT_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_market_portrait_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_polygon_settlement_backfill = define_asset_job(
    "polymarket_wc2026_polygon_settlement_backfill",
    selection=POLYMARKET_WC2026_POLYGON_SETTLEMENT_BACKFILL_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_polygon_settlement_backfill_run_config(),
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_polygon_settlement_release = define_asset_job(
    "polymarket_wc2026_polygon_settlement_release",
    selection=POLYMARKET_WC2026_POLYGON_SETTLEMENT_RELEASE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_POLYMARKET_WC2026_TAGS,
)

polymarket_wc2026_full_pipeline = define_asset_job(
    POLYMARKET_WC2026_SCOPE.full_job_name,
    selection=POLYMARKET_WC2026_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=polymarket_wc2026_full_pipeline_run_config(),
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
    KALSHI_WC2026_MARKET_REGISTRY_SELECTION
    | KALSHI_WC2026_HOURLY_ODDS_SELECTION
    | KALSHI_WC2026_DBT_SELECTION
)

kalshi_wc2026_market_scope_registry_refresh = define_asset_job(
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
