"""Polygon settlement types and constants."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from oddsfox_pipeline.config.settings import BASE_DIR
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    NEG_RISK_V2_EXCHANGE,
    STANDARD_V2_EXCHANGE,
    PolygonMarket,
)

NORMALIZER_VERSION = "polygon-v2-settlement-v4"
EXCHANGE_ADDRESSES = (
    STANDARD_V2_EXCHANGE.casefold(),
    NEG_RISK_V2_EXCHANGE.casefold(),
)
_VOLUME_QUANTUM = Decimal("0.000001")
_PRICE_QUANTUM = Decimal("0.000000000000000001")
_PRICE_SCALE = 10**18
# dbt computes exact 18-place ratios with UHUGEINT arithmetic.  This bound is
# the largest six-decimal volume accepted by that audited arithmetic path.
_MAX_RATIO_SAFE_VOLUME_UNSCALED = 340_282_366_920_938_463_374
_MIN_LOG_CHUNK_SIZE = 250
_MAX_LOG_CHUNK_SIZE = 20_000
_MIN_RECEIPT_BATCH_SIZE = 5
_MAX_RECEIPT_BATCH_SIZE = 50
_STATUS_ROOT = BASE_DIR / ".cache" / "polygon_settlement" / "status"


@dataclass(frozen=True)
class PolygonSettlementSyncConfig:
    requests_per_second: float = 5.0
    workers: int = 5
    initial_block_chunk_size: int = 8_000
    initial_receipt_batch_size: int = 20
    transient_retries: int = 4
    transient_backoff_seconds: float = 0.5
    progress_log_interval_seconds: int = 60
    no_progress_soft_timeout_seconds: int | None = 900
    no_progress_hard_timeout_seconds: int | None = 2_700

    def __post_init__(self) -> None:
        if self.requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        if self.workers <= 0:
            raise ValueError("workers must be positive")
        if (
            not _MIN_LOG_CHUNK_SIZE
            <= self.initial_block_chunk_size
            <= _MAX_LOG_CHUNK_SIZE
        ):
            raise ValueError("initial_block_chunk_size must be between 250 and 20000")
        if not (
            _MIN_RECEIPT_BATCH_SIZE
            <= self.initial_receipt_batch_size
            <= _MAX_RECEIPT_BATCH_SIZE
        ):
            raise ValueError("initial_receipt_batch_size must be between 5 and 50")
        if self.transient_retries < 0:
            raise ValueError("transient_retries must be non-negative")
        if self.transient_backoff_seconds < 0:
            raise ValueError("transient_backoff_seconds must be non-negative")


@dataclass(frozen=True)
class PolygonTargetRange:
    exchange_address: str
    from_block: int
    to_block: int
    from_block_hash: str
    to_block_hash: str

    def as_dict(self) -> dict[str, int | str]:
        return {
            "exchange_address": self.exchange_address,
            "from_block": self.from_block,
            "to_block": self.to_block,
            "from_block_hash": self.from_block_hash,
            "to_block_hash": self.to_block_hash,
        }


@dataclass(frozen=True)
class PolygonTokenTarget:
    market: PolygonMarket
    outcome_side: str
    exchange_address: str
    first_valid_block: int
    first_invalid_block: int


@dataclass(frozen=True)
class PolygonScanPlan:
    target_ranges: tuple[PolygonTargetRange, ...]
    token_targets: Mapping[str, PolygonTokenTarget]


@dataclass(frozen=True)
class PolygonChunkMetrics:
    duration_ms: int
    http_request_count: int
    log_rpc_call_count: int
    receipt_rpc_call_count: int
    header_rpc_call_count: int
    discovery_count: int
    eligible_discovery_count: int
    filtered_discovery_count: int
    receipt_transaction_count: int
    receipt_log_count: int
    retry_count: int
    adaptive_split_count: int

    def as_dict(self) -> dict[str, int]:
        return vars(self)


@dataclass(frozen=True)
class PolygonLeafResult:
    exchange_address: str
    from_block: int
    to_block: int
    from_block_hash: str
    to_block_hash: str
    rows: tuple[dict[str, Any], ...]
    scoped_event_count: int
    scoped_event_sha256: str
    event_count: int
    metrics: PolygonChunkMetrics
    next_log_chunk_size: int


@dataclass
class _RangeWork:
    target: PolygonTargetRange
    gaps: deque[tuple[int, int]]
    chunk_size: int
    cursor: int | None = None
    gap_end: int | None = None

    def next_chunk(self) -> tuple[int, int] | None:
        if self.cursor is None:
            if not self.gaps:
                return None
            self.cursor, self.gap_end = self.gaps.popleft()
        assert self.gap_end is not None
        start = self.cursor
        end = min(self.gap_end, start + self.chunk_size - 1)
        self.cursor = None if end == self.gap_end else end + 1
        if self.cursor is None:
            self.gap_end = None
        return start, end
