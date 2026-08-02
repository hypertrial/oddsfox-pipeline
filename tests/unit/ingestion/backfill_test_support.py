"""Shared patch targets for metadata enrichment unit tests."""

from __future__ import annotations

from oddsfox_pipeline.ingestion.polymarket.markets.backfill import (
    _events_fallback as bf_events_fallback,
)
from oddsfox_pipeline.ingestion.polymarket.markets.backfill import (
    _gamma as bf_gamma,
)
from oddsfox_pipeline.ingestion.polymarket.markets.backfill import (
    metadata as bf_metadata,
)

METADATA_ENTRYPOINT_MODULES = {
    "enrich_market_metadata": bf_metadata,
}


def patch_ensure_duck_db(monkeypatch) -> None:
    """Stub DuckDB init for metadata enrichment tests."""
    for mod in METADATA_ENTRYPOINT_MODULES.values():
        monkeypatch.setattr(mod, "ensure_duck_db", lambda: None)


__all__ = [
    "METADATA_ENTRYPOINT_MODULES",
    "bf_events_fallback",
    "bf_gamma",
    "bf_metadata",
    "patch_ensure_duck_db",
]
