"""OpenFootball WC2026 fixture ingestion."""

from oddsfox_pipeline.ingestion.openfootball.knockout_fixtures import (
    OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL,
    fetch_knockout_fixtures,
    parse_knockout_fixtures,
    sync_knockout_fixtures,
)

__all__ = [
    "OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL",
    "fetch_knockout_fixtures",
    "parse_knockout_fixtures",
    "sync_knockout_fixtures",
]
