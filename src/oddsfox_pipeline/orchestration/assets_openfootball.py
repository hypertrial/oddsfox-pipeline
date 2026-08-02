"""Dagster asset for the WC2026 schedule fixture mirror."""

from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)

from oddsfox_pipeline.ingestion.openfootball.schedule_fixtures import (
    OPENFOOTBALL_WC2026_SCHEDULE_FIXTURES_URL,
    sync_schedule_fixtures,
)
from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_OPENFOOTBALL, asset_key

OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES = asset_key(
    SOURCE_OPENFOOTBALL, SCOPE_WC2026, "raw", "schedule_fixtures"
)


@multi_asset(
    name="openfootball_wc2026_raw_schedule_fixtures",
    specs=[
        AssetSpec(
            key=OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES,
            deps=[],
        )
    ],
    group_name="ingestion",
)
def openfootball_wc2026_raw_schedule_fixtures(
    context: AssetExecutionContext,
) -> MaterializeResult:
    summary = sync_schedule_fixtures()
    context.log.info("OpenFootball WC2026 fixture sync summary: %s", summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.url(OPENFOOTBALL_WC2026_SCHEDULE_FIXTURES_URL),
            "rows": MetadataValue.int(int(summary["rows"])),
            "completed_rows": MetadataValue.int(int(summary["completed_rows"])),
            "scheduled_rows": MetadataValue.int(int(summary["scheduled_rows"])),
        }
    )


__all__ = [
    "OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES",
    "openfootball_wc2026_raw_schedule_fixtures",
]
