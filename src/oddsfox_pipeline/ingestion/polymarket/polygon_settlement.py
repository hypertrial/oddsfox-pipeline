"""Finalized Polygon V2 settlement backfill for the independent WC2026 seed."""

from __future__ import annotations

from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_normalize import (
    decode_and_normalize_leaf,
    normalize_v2_segment,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_scan import (
    build_polygon_target_ranges,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_sync import (
    sync_polygon_settlement_fills,
    verify_polygon_settlement_scan,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_types import (
    EXCHANGE_ADDRESSES,
    NORMALIZER_VERSION,
    PolygonSettlementSyncConfig,
    PolygonTargetRange,
)

__all__ = [
    "EXCHANGE_ADDRESSES",
    "NORMALIZER_VERSION",
    "PolygonSettlementSyncConfig",
    "PolygonTargetRange",
    "build_polygon_target_ranges",
    "decode_and_normalize_leaf",
    "normalize_v2_segment",
    "sync_polygon_settlement_fills",
    "verify_polygon_settlement_scan",
]
