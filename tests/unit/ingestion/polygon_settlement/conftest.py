from __future__ import annotations

import os
import types
from collections import deque
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from tests.unit.ingestion.test_polygon_seed import complete_seed_rows

import oddsfox_pipeline.ingestion.polymarket.polygon_settlement as polygon_settlement
import oddsfox_pipeline.ingestion.polymarket.polygon_settlement_normalize as polygon_settlement_normalize
import oddsfox_pipeline.ingestion.polymarket.polygon_settlement_scan as polygon_settlement_scan
import oddsfox_pipeline.ingestion.polymarket.polygon_settlement_sync as polygon_settlement_sync
import oddsfox_pipeline.ingestion.polymarket.polygon_settlement_types as polygon_settlement_types
from oddsfox_pipeline.config.settings import BASE_DIR
from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import DecodedSettlementEvent
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    STANDARD_V2_EXCHANGE,
    PolygonMarketManifest,
    parse_polygon_market,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (
    normalize_v2_segment,
)
from oddsfox_pipeline.storage.duckdb import (
    polygon_settlement as polygon_settlement_storage,
)


def build_manifest() -> PolygonMarketManifest:
    rows = complete_seed_rows()
    standard = replace(
        parse_polygon_market(rows[0]),
        market_structure="standard",
        exchange_address=STANDARD_V2_EXCHANGE,
    )
    neg_risk = parse_polygon_market(rows[1])
    return PolygonMarketManifest(
        markets=(standard, neg_risk),
        sha256=standard.manifest_sha256,
        version=standard.manifest_version,
    )


@pytest.fixture
def manifest() -> PolygonMarketManifest:
    return build_manifest()


def event(
    kind: str,
    side: str,
    token_id: str,
    maker: int,
    taker: int,
    log_index: int,
    *,
    transaction_index: int = 0,
) -> DecodedSettlementEvent:
    return DecodedSettlementEvent(
        kind=kind,
        exchange_address=STANDARD_V2_EXCHANGE.casefold(),
        block_number=100,
        block_hash="0x" + "1" * 64,
        transaction_hash="0x" + "2" * 64,
        transaction_index=transaction_index,
        log_index=log_index,
        side=side,
        token_id=token_id,
        maker_amount=maker,
        taker_amount=taker,
    )


def normalize(passive, active, matched, *, offset_minutes: int = 1):
    manifest = build_manifest()
    market = manifest.markets[0]
    return normalize_v2_segment(
        passive,
        active,
        matched,
        manifest=manifest,
        scan_id="scan",
        from_block=99,
        to_block=101,
        block_timestamp=market.window_start_at_utc + timedelta(minutes=offset_minutes),
        ingested_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )


polygon_settlement_module = types.SimpleNamespace(
    BASE_DIR=BASE_DIR,
    os=os,
    deque=deque,
    CHUNKS_TABLE=polygon_settlement_storage.CHUNKS_TABLE,
    FILLS_TABLE=polygon_settlement_storage.FILLS_TABLE,
    RUNS_TABLE=polygon_settlement_storage.RUNS_TABLE,
    STAGE_TABLE=polygon_settlement_storage.STAGE_TABLE,
    EXCHANGE_ADDRESSES=polygon_settlement_types.EXCHANGE_ADDRESSES,
    NORMALIZER_VERSION=polygon_settlement_types.NORMALIZER_VERSION,
    PolygonSettlementSyncConfig=polygon_settlement_types.PolygonSettlementSyncConfig,
    PolygonTargetRange=polygon_settlement_types.PolygonTargetRange,
    PolygonTokenTarget=polygon_settlement_types.PolygonTokenTarget,
    PolygonScanPlan=polygon_settlement_types.PolygonScanPlan,
    _RangeWork=polygon_settlement_types._RangeWork,
    _STATUS_ROOT=polygon_settlement_types._STATUS_ROOT,
    build_polygon_scan_plan=polygon_settlement_scan.build_polygon_scan_plan,
    build_polygon_target_ranges=polygon_settlement.build_polygon_target_ranges,
    decode_and_normalize_leaf=polygon_settlement.decode_and_normalize_leaf,
    discover_and_normalize_leaf=polygon_settlement_normalize.discover_and_normalize_leaf,
    normalize_v2_segment=polygon_settlement.normalize_v2_segment,
    sync_polygon_settlement_fills=polygon_settlement.sync_polygon_settlement_fills,
    verify_polygon_settlement_scan=polygon_settlement.verify_polygon_settlement_scan,
    _amounts=polygon_settlement_normalize._amounts,
    _collect_and_normalize_leaf=polygon_settlement_scan._collect_and_normalize_leaf,
    _collect_parent_range=polygon_settlement_scan._collect_parent_range,
    _concurrent_leaf_results=polygon_settlement_scan._concurrent_leaf_results,
    _decimal_price=polygon_settlement_normalize._decimal_price,
    _decimal_volume=polygon_settlement_normalize._decimal_volume,
    _event_payload=polygon_settlement_normalize._event_payload,
    _fetch_receipts_adaptively=polygon_settlement_scan._fetch_receipts_adaptively,
    _gaps=polygon_settlement_scan._gaps,
    _incremental_scoped_hash=polygon_settlement_scan._incremental_scoped_hash,
    _offline_published_summary=polygon_settlement_sync._offline_published_summary,
    _parse_target_ranges=polygon_settlement_scan._parse_target_ranges,
    _revalidate_resumed_chunk_headers=polygon_settlement_scan._revalidate_resumed_chunk_headers,
    _sha256_json=polygon_settlement_normalize._sha256_json,
    _write_status=polygon_settlement_sync._write_status,
)
