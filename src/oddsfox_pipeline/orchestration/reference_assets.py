"""External Scraper-produced reference assets consumed read-only by Pipeline."""

from dagster import AssetKey, AssetSpec

from oddsfox_pipeline.contracts.reference_bundle import REFERENCE_TABLE_PRIMARY_KEYS

REFERENCE_WC2026_FIXTURES = AssetKey(
    ["oddsfox", "reference", "openfootball_wc2026_schedule_fixtures"]
)
reference_assets = tuple(
    AssetSpec(key=AssetKey(["oddsfox", "reference", table]))
    for table in REFERENCE_TABLE_PRIMARY_KEYS
)

__all__ = ["REFERENCE_WC2026_FIXTURES", "reference_assets"]
