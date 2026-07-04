from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from oddsfox_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)
from oddsfox_pipeline.orchestration.assets import (
    wc2026_polymarket_dbt,
    wc2026_polymarket_market_metadata_backfill,
    wc2026_polymarket_market_registry,
    wc2026_polymarket_markets_snapshot,
    wc2026_polymarket_raw_markets,
    wc2026_polymarket_token_odds_history_hourly,
)
from oddsfox_pipeline.orchestration.jobs import (
    wc2026_dbt_build,
    wc2026_full_pipeline,
    wc2026_hourly_odds_ingest,
    wc2026_market_registry_refresh,
)
from oddsfox_pipeline.orchestration.schedules import wc2026_hourly_odds_schedule

defs = Definitions(
    assets=[
        wc2026_polymarket_raw_markets,
        wc2026_polymarket_markets_snapshot,
        wc2026_polymarket_market_registry,
        wc2026_polymarket_market_metadata_backfill,
        wc2026_polymarket_token_odds_history_hourly,
        wc2026_polymarket_dbt,
    ],
    jobs=[
        wc2026_hourly_odds_ingest,
        wc2026_market_registry_refresh,
        wc2026_dbt_build,
        wc2026_full_pipeline,
    ],
    schedules=[
        wc2026_hourly_odds_schedule,
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
