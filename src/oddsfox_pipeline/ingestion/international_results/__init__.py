"""International-results CSV ingestion."""

from oddsfox_pipeline.ingestion.international_results.historical import (
    parse_historical_csvs,
    sync_historical_international_results,
)
from oddsfox_pipeline.ingestion.international_results.match_results import (
    INTERNATIONAL_RESULTS_CSV_URL,
    parse_wc2026_match_results_csv,
    sync_wc2026_match_results,
)

__all__ = [
    "INTERNATIONAL_RESULTS_CSV_URL",
    "parse_historical_csvs",
    "parse_wc2026_match_results_csv",
    "sync_historical_international_results",
    "sync_wc2026_match_results",
]
