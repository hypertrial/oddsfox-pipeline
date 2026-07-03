from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from oddsfox_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)
from oddsfox_pipeline.orchestration.assets import (
    polymarket_dbt,
    polymarket_market_metadata_backfill,
    polymarket_market_scope_registry,
    polymarket_markets_raw_dlt,
    polymarket_markets_snapshot,
    polymarket_odds_repair,
    polymarket_token_odds_history,
    polymarket_token_odds_history_hourly,
    polymarket_token_odds_history_minutely,
)
from oddsfox_pipeline.orchestration.jobs import (
    dbt_full_refresh,
    polymarket_hourly_odds_ingest,
    polymarket_ingest_full_refresh_events,
    polymarket_ingest_incremental,
    polymarket_minutely_odds_ingest,
    polymarket_selected_scope_full_pipeline,
)
from oddsfox_pipeline.orchestration.schedules import (
    polymarket_hourly_odds_schedule,
    polymarket_minutely_odds_cold_schedule,
    polymarket_minutely_odds_live_schedule,
    polymarket_minutely_odds_schedule,
)

defs = Definitions(
    assets=[
        polymarket_markets_raw_dlt,
        polymarket_markets_snapshot,
        polymarket_market_scope_registry,
        polymarket_market_metadata_backfill,
        polymarket_token_odds_history,
        polymarket_token_odds_history_minutely,
        polymarket_token_odds_history_hourly,
        polymarket_odds_repair,
        polymarket_dbt,
    ],
    jobs=[
        polymarket_ingest_incremental,
        polymarket_ingest_full_refresh_events,
        polymarket_minutely_odds_ingest,
        polymarket_hourly_odds_ingest,
        dbt_full_refresh,
        polymarket_selected_scope_full_pipeline,
    ],
    schedules=[
        polymarket_minutely_odds_schedule,
        polymarket_minutely_odds_cold_schedule,
        polymarket_minutely_odds_live_schedule,
        polymarket_hourly_odds_schedule,
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
