from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from oddsfox_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)
from oddsfox_pipeline.orchestration.assets import (
    polymarket_wc2026_dbt,
    polymarket_wc2026_ops_market_scope_registry,
    polymarket_wc2026_raw_market_metadata_backfill,
    polymarket_wc2026_raw_markets,
    polymarket_wc2026_raw_markets_snapshot,
    polymarket_wc2026_raw_token_odds_history_hourly,
)
from oddsfox_pipeline.orchestration.jobs import (
    polymarket_wc2026_dbt_build,
    polymarket_wc2026_full_pipeline,
    polymarket_wc2026_hourly_odds_ingest,
    polymarket_wc2026_market_registry_refresh,
)
from oddsfox_pipeline.orchestration.schedules import (
    polymarket_wc2026_hourly_odds_schedule,
)

defs = Definitions(
    assets=[
        polymarket_wc2026_raw_markets,
        polymarket_wc2026_raw_markets_snapshot,
        polymarket_wc2026_ops_market_scope_registry,
        polymarket_wc2026_raw_market_metadata_backfill,
        polymarket_wc2026_raw_token_odds_history_hourly,
        polymarket_wc2026_dbt,
    ],
    jobs=[
        polymarket_wc2026_hourly_odds_ingest,
        polymarket_wc2026_market_registry_refresh,
        polymarket_wc2026_dbt_build,
        polymarket_wc2026_full_pipeline,
    ],
    schedules=[
        polymarket_wc2026_hourly_odds_schedule,
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
