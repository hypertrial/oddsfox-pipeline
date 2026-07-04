from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)

from oddsfox_pipeline.ingestion.international_results.match_results import (
    INTERNATIONAL_RESULTS_CSV_URL,
    sync_wc2026_match_results,
)
from oddsfox_pipeline.naming import (
    SCOPE_WC2026,
    SOURCE_INTERNATIONAL_RESULTS,
    asset_key,
)

INTERNATIONAL_RESULTS_WC2026_RAW_MATCH_RESULTS = asset_key(
    SOURCE_INTERNATIONAL_RESULTS, SCOPE_WC2026, "raw", "match_results"
)


@multi_asset(
    name="international_results_wc2026_raw_match_results",
    specs=[
        AssetSpec(
            key=INTERNATIONAL_RESULTS_WC2026_RAW_MATCH_RESULTS,
            deps=[],
        )
    ],
    group_name="ingestion",
)
def international_results_wc2026_raw_match_results(
    context: AssetExecutionContext,
) -> MaterializeResult:
    summary = sync_wc2026_match_results()
    context.log.info("international_results WC2026 sync summary: %s", summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.url(INTERNATIONAL_RESULTS_CSV_URL),
            "rows": MetadataValue.int(int(summary["rows"])),
            "completed_rows": MetadataValue.int(int(summary["completed_rows"])),
            "scheduled_rows": MetadataValue.int(int(summary["scheduled_rows"])),
        }
    )


__all__ = [
    "INTERNATIONAL_RESULTS_WC2026_RAW_MATCH_RESULTS",
    "international_results_wc2026_raw_match_results",
]
