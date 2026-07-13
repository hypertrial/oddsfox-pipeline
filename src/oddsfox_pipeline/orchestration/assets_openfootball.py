"""Dagster asset for the WC2026 knockout fixture mirror."""

from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)

from oddsfox_pipeline.ingestion.openfootball.knockout_fixtures import (
    OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL,
    sync_knockout_fixtures,
)
from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_OPENFOOTBALL, asset_key

OPENFOOTBALL_WC2026_RAW_KNOCKOUT_FIXTURES = asset_key(
    SOURCE_OPENFOOTBALL, SCOPE_WC2026, "raw", "knockout_fixtures"
)


@multi_asset(
    name="openfootball_wc2026_raw_knockout_fixtures",
    specs=[
        AssetSpec(
            key=OPENFOOTBALL_WC2026_RAW_KNOCKOUT_FIXTURES,
            deps=[],
        )
    ],
    group_name="ingestion",
)
def openfootball_wc2026_raw_knockout_fixtures(
    context: AssetExecutionContext,
) -> MaterializeResult:
    summary = sync_knockout_fixtures()
    context.log.info("OpenFootball WC2026 fixture sync summary: %s", summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.url(OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL),
            "rows": MetadataValue.int(int(summary["rows"])),
            "completed_rows": MetadataValue.int(int(summary["completed_rows"])),
            "scheduled_rows": MetadataValue.int(int(summary["scheduled_rows"])),
        }
    )


__all__ = [
    "OPENFOOTBALL_WC2026_RAW_KNOCKOUT_FIXTURES",
    "openfootball_wc2026_raw_knockout_fixtures",
]
