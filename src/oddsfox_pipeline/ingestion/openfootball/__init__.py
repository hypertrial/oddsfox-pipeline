"""OpenFootball WC2026 fixture ingestion."""

from oddsfox_pipeline.ingestion.openfootball.schedule_fixtures import (
    OPENFOOTBALL_WC2026_SCHEDULE_FIXTURES_URL,
    fetch_schedule_fixtures,
    parse_schedule_fixtures,
    sync_schedule_fixtures,
)

__all__ = [
    "OPENFOOTBALL_WC2026_SCHEDULE_FIXTURES_URL",
    "fetch_schedule_fixtures",
    "parse_schedule_fixtures",
    "sync_schedule_fixtures",
]
