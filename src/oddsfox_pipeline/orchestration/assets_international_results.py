from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)

from oddsfox_pipeline.ingestion.international_results.historical import (
    GOALSCORERS_URL,
    RESULTS_URL,
    SHOOTOUTS_URL,
    sync_historical_international_results,
)
from oddsfox_pipeline.ingestion.international_results.match_results import (
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
INTERNATIONAL_RESULTS_HISTORICAL_RAW = asset_key(
    SOURCE_INTERNATIONAL_RESULTS, "historical", "raw", "snapshot"
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
            "source": MetadataValue.url(str(summary["source_url"])),
            "source_revision": MetadataValue.text(str(summary["source_revision"])),
            "source_payload_sha256": MetadataValue.text(
                str(summary["source_payload_sha256"])
            ),
            "rows": MetadataValue.int(int(summary["rows"])),
            "completed_rows": MetadataValue.int(int(summary["completed_rows"])),
            "scheduled_rows": MetadataValue.int(int(summary["scheduled_rows"])),
        }
    )


@multi_asset(
    name="international_results_historical_raw_snapshot",
    specs=[
        AssetSpec(
            key=INTERNATIONAL_RESULTS_HISTORICAL_RAW,
            deps=[],
        )
    ],
    group_name="ingestion",
)
def international_results_historical_raw_snapshot(
    context: AssetExecutionContext,
) -> MaterializeResult:
    summary = sync_historical_international_results()
    context.log.info("international_results historical sync summary: %s", summary)
    return MaterializeResult(
        metadata={
            "results_source": MetadataValue.url(RESULTS_URL),
            "shootouts_source": MetadataValue.url(SHOOTOUTS_URL),
            "goalscorers_source": MetadataValue.url(GOALSCORERS_URL),
            "matches": MetadataValue.int(int(summary["inserted_matches"])),
            "shootouts": MetadataValue.int(int(summary["inserted_shootouts"])),
            "goalscorers": MetadataValue.int(int(summary["inserted_goalscorers"])),
        }
    )


__all__ = [
    "INTERNATIONAL_RESULTS_HISTORICAL_RAW",
    "INTERNATIONAL_RESULTS_WC2026_RAW_MATCH_RESULTS",
    "international_results_historical_raw_snapshot",
    "international_results_wc2026_raw_match_results",
]
