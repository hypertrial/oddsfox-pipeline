from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from oddsfox_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)
from oddsfox_pipeline.orchestration.assets import (
    international_results_wc2026_raw_match_results,
    kalshi_wc2026_ops_market_scope_registry,
    kalshi_wc2026_raw_market_candlesticks_hourly,
    kalshi_wc2026_raw_markets,
    kalshi_wc2026_raw_markets_snapshot,
    oddsfox_dbt,
    openfootball_wc2026_raw_knockout_fixtures,
    polymarket_us_midterms_2026_ops_market_scope_registry,
    polymarket_us_midterms_2026_raw_market_metadata_backfill,
    polymarket_us_midterms_2026_raw_markets,
    polymarket_us_midterms_2026_raw_markets_snapshot,
    polymarket_us_midterms_2026_raw_token_odds_history_hourly,
    polymarket_wc2026_ops_market_scope_registry,
    polymarket_wc2026_raw_market_metadata_backfill,
    polymarket_wc2026_raw_markets,
    polymarket_wc2026_raw_markets_snapshot,
    polymarket_wc2026_raw_token_odds_history_hourly,
)
from oddsfox_pipeline.orchestration.jobs import (
    international_results_wc2026_match_results_ingest,
    kalshi_wc2026_dbt_build,
    kalshi_wc2026_full_pipeline,
    kalshi_wc2026_hourly_odds_ingest,
    kalshi_wc2026_market_registry_refresh,
    polymarket_us_midterms_2026_dbt_build,
    polymarket_us_midterms_2026_full_pipeline,
    polymarket_us_midterms_2026_hourly_odds_ingest,
    polymarket_us_midterms_2026_market_registry_refresh,
    polymarket_wc2026_dbt_build,
    polymarket_wc2026_full_pipeline,
    polymarket_wc2026_hourly_odds_ingest,
    polymarket_wc2026_market_registry_refresh,
    wc2026_knockout_match_odds_full_pipeline,
)
from oddsfox_pipeline.orchestration.schedules import (
    kalshi_wc2026_hourly_odds_schedule,
    polymarket_us_midterms_2026_hourly_odds_schedule,
    polymarket_wc2026_hourly_odds_schedule,
    wc2026_knockout_match_odds_hourly_schedule,
)

defs = Definitions(
    assets=[
        international_results_wc2026_raw_match_results,
        openfootball_wc2026_raw_knockout_fixtures,
        kalshi_wc2026_raw_markets,
        kalshi_wc2026_raw_markets_snapshot,
        kalshi_wc2026_ops_market_scope_registry,
        kalshi_wc2026_raw_market_candlesticks_hourly,
        polymarket_wc2026_raw_markets,
        polymarket_wc2026_raw_markets_snapshot,
        polymarket_wc2026_ops_market_scope_registry,
        polymarket_wc2026_raw_market_metadata_backfill,
        polymarket_wc2026_raw_token_odds_history_hourly,
        polymarket_us_midterms_2026_raw_markets,
        polymarket_us_midterms_2026_raw_markets_snapshot,
        polymarket_us_midterms_2026_ops_market_scope_registry,
        polymarket_us_midterms_2026_raw_market_metadata_backfill,
        polymarket_us_midterms_2026_raw_token_odds_history_hourly,
        oddsfox_dbt,
    ],
    jobs=[
        international_results_wc2026_match_results_ingest,
        kalshi_wc2026_hourly_odds_ingest,
        kalshi_wc2026_market_registry_refresh,
        kalshi_wc2026_dbt_build,
        kalshi_wc2026_full_pipeline,
        polymarket_wc2026_hourly_odds_ingest,
        polymarket_wc2026_market_registry_refresh,
        polymarket_wc2026_dbt_build,
        polymarket_wc2026_full_pipeline,
        polymarket_us_midterms_2026_hourly_odds_ingest,
        polymarket_us_midterms_2026_market_registry_refresh,
        polymarket_us_midterms_2026_dbt_build,
        polymarket_us_midterms_2026_full_pipeline,
        wc2026_knockout_match_odds_full_pipeline,
    ],
    schedules=[
        kalshi_wc2026_hourly_odds_schedule,
        polymarket_wc2026_hourly_odds_schedule,
        polymarket_us_midterms_2026_hourly_odds_schedule,
        wc2026_knockout_match_odds_hourly_schedule,
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
            profile="oddsfox",
            target="dev",
            dbt_executable=resolve_dbt_executable(),
        ),
        "dlt": DagsterDltResource(),
    },
)
