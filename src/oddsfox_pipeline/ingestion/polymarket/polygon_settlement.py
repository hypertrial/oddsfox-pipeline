"""Finalized Polygon V2 settlement backfill for the independent WC2026 seed."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from collections import defaultdict, deque
from collections.abc import Iterator, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from threading import local
from time import monotonic
from typing import Any, Iterable, Sequence

import duckdb

from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import (
    EVENT_TOPICS,
    ORDERS_MATCHED_TOPIC,
    DecodedSettlementEvent,
    PolygonBlock,
    PolygonRPC,
    PolygonRPCError,
    PolygonRPCMetrics,
    PolygonRPCSizeLimitError,
    adaptive_log_leaves,
    decode_settlement_log,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    DEFAULT_POLYGON_MARKET_SEED_PATH,
    NEG_RISK_V2_EXCHANGE,
    POLYGON_CHAIN_ID,
    STANDARD_V2_EXCHANGE,
    PolygonMarket,
    PolygonMarketManifest,
    load_polygon_market_seed,
)
from oddsfox_pipeline.resources.http import RateLimiter
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (
    CHUNKS_TABLE,
    FILLS_TABLE,
    RUNS_TABLE,
    STAGE_TABLE,
    completed_polygon_chunk_ranges,
    publish_polygon_settlement_scan,
    record_polygon_settlement_chunk,
    record_polygon_settlement_failure,
    set_polygon_verification_status,
    start_polygon_settlement_scan,
    validate_polygon_provider_label,
)

logger = logging.getLogger(__name__)

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
_STATUS_ROOT = (
    Path(__file__).resolve().parents[4] / ".cache" / "polygon_settlement" / "status"
)


def _block_headers(rpc: PolygonRPC, numbers: Iterable[int]) -> dict[int, PolygonBlock]:
    """Use production batching while keeping injected replay clients minimal."""
    requested = tuple(dict.fromkeys(numbers))
    batch = getattr(rpc, "blocks", None)
    if callable(batch):
        return batch(requested)
    return {number: rpc.block(number) for number in requested}


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


def _decimal_volume(value: int) -> Decimal:
    if value <= 0 or value > _MAX_RATIO_SAFE_VOLUME_UNSCALED:
        raise ValueError("Settlement volume exceeds the exact-ratio safe bound")
    return Decimal(value).scaleb(-6).quantize(_VOLUME_QUANTUM)


def _decimal_price(collateral: int, shares: int) -> Decimal:
    if shares <= 0 or collateral <= 0 or collateral > shares:
        raise ValueError(
            "Settlement price inputs must satisfy 0 < collateral <= shares"
        )
    scaled, remainder = divmod(collateral * _PRICE_SCALE, shares)
    doubled = remainder * 2
    if doubled > shares or (doubled == shares and scaled % 2):
        scaled += 1
    return Decimal(scaled).scaleb(-18).quantize(_PRICE_QUANTUM)


def _event_payload(event: DecodedSettlementEvent) -> tuple[Any, ...]:
    """Sanitized event identity used for provider comparison and audit hashes."""
    return (
        event.kind,
        event.exchange_address,
        event.block_number,
        event.block_hash,
        event.transaction_hash,
        event.transaction_index,
        event.log_index,
        event.side,
        event.token_id,
        str(event.maker_amount),
        str(event.taker_amount),
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def build_polygon_scan_plan(
    rpc: PolygonRPC,
    manifest: PolygonMarketManifest,
    finalized_head: PolygonBlock,
) -> PolygonScanPlan:
    """Resolve each unique window once, then merge only within its exchange."""
    windows = sorted(
        {
            (market.window_start_at_utc, market.window_end_at_utc)
            for market in manifest.markets
        }
    )
    batch_search = getattr(rpc, "first_blocks_at_or_after", None)
    if callable(batch_search):
        raw_boundaries = batch_search(
            (timestamp for window in windows for timestamp in window),
            finalized_head=finalized_head,
        )
        if len(raw_boundaries) != len(windows) * 2:
            raise RuntimeError("Polygon boundary search returned the wrong count")
        window_blocks = {
            window: (raw_boundaries[index], raw_boundaries[index + 1])
            for index, window in zip(
                range(0, len(raw_boundaries), 2), windows, strict=True
            )
        }
    else:
        window_blocks: dict[tuple[datetime, datetime], tuple[int, int]] = {}
        low = 0
        for start, end in windows:
            first = rpc.first_block_at_or_after(
                start, finalized_head=finalized_head, low=low
            )
            boundary = rpc.first_block_at_or_after(
                end, finalized_head=finalized_head, low=first
            )
            window_blocks[(start, end)] = (first, boundary)
            low = first

    token_targets: dict[str, PolygonTokenTarget] = {}
    windows_by_exchange: dict[str, set[tuple[datetime, datetime]]] = defaultdict(set)
    for market in manifest.markets:
        window = (market.window_start_at_utc, market.window_end_at_utc)
        first, boundary = window_blocks[window]
        if not 0 <= first < boundary <= finalized_head.number:
            raise RuntimeError("Polygon analysis window has invalid finalized bounds")
        address = market.exchange_address.casefold()
        windows_by_exchange[address].add(window)
        for token_id, outcome in (
            (market.yes_token_id, "yes"),
            (market.no_token_id, "no"),
        ):
            token_targets[token_id] = PolygonTokenTarget(
                market=market,
                outcome_side=outcome,
                exchange_address=address,
                first_valid_block=first,
                first_invalid_block=boundary,
            )

    numeric: list[tuple[str, int, int]] = []
    for address in EXCHANGE_ADDRESSES:
        merged: list[tuple[int, int]] = []
        for window in sorted(windows_by_exchange.get(address, set())):
            first, boundary = window_blocks[window]
            candidate = (max(0, first - 1), boundary)
            if merged and candidate[0] <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], candidate[1]))
            else:
                merged.append(candidate)
        numeric.extend((address, start, end) for start, end in merged)
    if not numeric:
        raise RuntimeError("Polygon manifest produced no target block ranges")
    headers = _block_headers(
        rpc,
        (
            number
            for _address, from_block, to_block in numeric
            for number in (from_block, to_block)
        ),
    )
    return PolygonScanPlan(
        target_ranges=tuple(
            PolygonTargetRange(
                exchange_address=address,
                from_block=from_block,
                to_block=to_block,
                from_block_hash=headers[from_block].hash,
                to_block_hash=headers[to_block].hash,
            )
            for address, from_block, to_block in numeric
        ),
        token_targets=token_targets,
    )


def build_polygon_target_ranges(
    rpc: PolygonRPC,
    manifest: PolygonMarketManifest,
    finalized_head: PolygonBlock,
) -> tuple[PolygonTargetRange, ...]:
    return build_polygon_scan_plan(rpc, manifest, finalized_head).target_ranges


def _segment_hash(
    passive: Sequence[DecodedSettlementEvent],
    active: DecodedSettlementEvent,
    matched: DecodedSettlementEvent,
) -> str:
    return _sha256_json(
        {
            "passive": [_event_payload(event) for event in passive],
            "active": _event_payload(active),
            "matched": _event_payload(matched),
        }
    )


def _amounts(event: DecodedSettlementEvent) -> tuple[int, int]:
    shares, collateral = (
        (event.taker_amount, event.maker_amount)
        if event.side == "BUY"
        else (event.maker_amount, event.taker_amount)
    )
    if shares <= 0 or collateral <= 0 or collateral > shares:
        raise ValueError("V2 fill has invalid share/collateral amounts")
    return shares, collateral


def _base_fill_row(
    *,
    scan_id: str,
    from_block: int,
    to_block: int,
    event: DecodedSettlementEvent,
    active: DecodedSettlementEvent,
    matched: DecodedSettlementEvent,
    market: PolygonMarket,
    token_id: str,
    outcome_side: str,
    order_side: str,
    shares: int,
    collateral: int,
    normalization_kind: str,
    is_derived: bool,
    ordinal: int,
    segment_sha256: str,
    block_timestamp: datetime,
    ingested_at: datetime,
) -> dict[str, Any]:
    return {
        "scan_id": scan_id,
        "chain_id": POLYGON_CHAIN_ID,
        "exchange_address": event.exchange_address,
        "chunk_from_block": from_block,
        "chunk_to_block": to_block,
        "block_number": event.block_number,
        "block_hash": event.block_hash,
        "block_timestamp": block_timestamp,
        "transaction_hash": event.transaction_hash,
        "transaction_index": event.transaction_index,
        "passive_log_index": event.log_index,
        "active_log_index": active.log_index,
        "matched_log_index": matched.log_index,
        "normalized_leg_ordinal": ordinal,
        "proposition_id": market.proposition_id,
        "condition_id": market.condition_id,
        "token_id": token_id,
        "outcome_side": outcome_side,
        "order_side": order_side,
        "source_token_id": event.token_id,
        "source_maker_amount": str(event.maker_amount),
        "source_taker_amount": str(event.taker_amount),
        "share_volume": _decimal_volume(shares),
        "gross_collateral_volume": _decimal_volume(collateral),
        "price": _decimal_price(collateral, shares),
        "normalization_kind": normalization_kind,
        "is_derived": is_derived,
        "segment_sha256": segment_sha256,
        "decoder_version": NORMALIZER_VERSION,
        "ingested_at": ingested_at,
    }


def normalize_v2_segment(
    passive: Sequence[DecodedSettlementEvent],
    active: DecodedSettlementEvent,
    matched: DecodedSettlementEvent,
    *,
    manifest: PolygonMarketManifest,
    scan_id: str,
    from_block: int,
    to_block: int,
    block_timestamp: datetime,
    ingested_at: datetime,
    token_index: Mapping[str, tuple[PolygonMarket, str]] | None = None,
) -> list[dict[str, Any]]:
    """Validate one taker aggregate and emit non-double-counted economic legs."""
    if not passive:
        raise ValueError("OrdersMatched must be preceded by passive OrderFilled legs")
    if active.kind != "order_filled" or matched.kind != "orders_matched":
        raise ValueError("Malformed V2 settlement segment boundary")
    segment_events = (*passive, active, matched)
    transaction_location = (
        active.exchange_address,
        active.block_number,
        active.block_hash,
        active.transaction_hash,
        active.transaction_index,
    )
    if any(
        (
            event.exchange_address,
            event.block_number,
            event.block_hash,
            event.transaction_hash,
            event.transaction_index,
        )
        != transaction_location
        for event in segment_events
    ):
        raise ValueError("V2 settlement segment crosses transaction boundaries")
    if (
        active.side,
        active.token_id,
        active.maker_amount,
        active.taker_amount,
    ) != (
        matched.side,
        matched.token_id,
        matched.maker_amount,
        matched.taker_amount,
    ):
        raise ValueError("OrdersMatched does not exactly match the active aggregate")

    token_index = token_index or manifest.by_token
    target_events = [
        event for event in (*passive, active) if event.token_id in token_index
    ]
    if not target_events:
        return []
    if active.token_id not in token_index:
        raise ValueError("Target passive fill has an unregistered active counterpart")
    active_market, active_outcome = token_index[active.token_id]
    if active.exchange_address != active_market.exchange_address.casefold():
        raise ValueError("Manifest market is assigned to the wrong V2 exchange")

    segment_sha256 = _segment_hash(passive, active, matched)
    rows: list[dict[str, Any]] = []
    expected_active_shares = 0
    expected_active_collateral = 0
    has_paired_leg = False
    for event in passive:
        mapped = token_index.get(event.token_id)
        if mapped is None:
            raise ValueError("Target segment contains an unregistered passive token")
        market, outcome = mapped
        if market.proposition_id != active_market.proposition_id:
            raise ValueError("V2 segment crosses independent market conditions")
        shares, collateral = _amounts(event)
        expected_active_shares += shares

        if event.token_id == active.token_id and event.side != active.side:
            kind = "complementary"
            expected_active_collateral += collateral
            derived = None
        else:
            complement = (
                active_market.no_token_id
                if active_outcome == "yes"
                else active_market.yes_token_id
            )
            if event.token_id != complement or event.side != active.side:
                raise ValueError("Unsupported V2 target-market match shape")
            kind = "mint" if active.side == "BUY" else "merge"
            has_paired_leg = True
            derived = shares - collateral
            if derived <= 0:
                raise ValueError("V2 derived collateral must be positive")
            expected_active_collateral += derived

        # Pinned V2 Trading._settleMakerOrders chooses MatchType per maker.
        # One OrdersMatched aggregate may therefore combine complementary legs
        # with MINT (BUY taker) or MERGE (SELL taker) legs.

        rows.append(
            _base_fill_row(
                scan_id=scan_id,
                from_block=from_block,
                to_block=to_block,
                event=event,
                active=active,
                matched=matched,
                market=market,
                token_id=event.token_id,
                outcome_side=outcome,
                order_side=event.side,
                shares=shares,
                collateral=collateral,
                normalization_kind=kind,
                is_derived=False,
                ordinal=0,
                segment_sha256=segment_sha256,
                block_timestamp=block_timestamp,
                ingested_at=ingested_at,
            )
        )
        if derived is not None:
            rows.append(
                _base_fill_row(
                    scan_id=scan_id,
                    from_block=from_block,
                    to_block=to_block,
                    event=event,
                    active=active,
                    matched=matched,
                    market=active_market,
                    token_id=active.token_id,
                    outcome_side=active_outcome,
                    order_side=active.side,
                    shares=shares,
                    collateral=derived,
                    normalization_kind=kind,
                    is_derived=True,
                    ordinal=1,
                    segment_sha256=segment_sha256,
                    block_timestamp=block_timestamp,
                    ingested_at=ingested_at,
                )
            )

    active_shares, active_collateral = _amounts(active)
    # The pinned V2 exchange emits the active order's requested maker fill, then
    # refunds any maker-asset surplus after mixed MINT/MERGE settlement.  The
    # passive events reconstruct the amount actually consumed.  Consequently a
    # BUY may refund collateral and a SELL may refund outcome shares; the other
    # (received) dimension must still reconcile exactly.  The all-complementary
    # fast path has no such refund and remains exact in both dimensions.
    if not has_paired_leg:
        aggregate_conserves = (
            active_shares == expected_active_shares
            and active_collateral == expected_active_collateral
        )
    elif active.side == "BUY":
        aggregate_conserves = (
            active_shares == expected_active_shares
            and active_collateral >= expected_active_collateral
        )
    else:
        aggregate_conserves = (
            active_shares >= expected_active_shares
            and active_collateral == expected_active_collateral
        )
    if not aggregate_conserves:
        raise ValueError("V2 active aggregate does not conserve normalized amounts")
    if not (
        active_market.window_start_at_utc
        <= block_timestamp
        < active_market.window_end_at_utc
    ):
        return []
    return rows


def _transaction_segments(
    events: Sequence[DecodedSettlementEvent],
) -> Iterable[
    tuple[
        tuple[DecodedSettlementEvent, ...],
        DecodedSettlementEvent,
        DecodedSettlementEvent,
    ]
]:
    pending: list[DecodedSettlementEvent] = []
    for event in events:
        if event.kind == "order_filled":
            pending.append(event)
            continue
        if len(pending) < 2:
            raise ValueError("OrdersMatched has no passive and active OrderFilled legs")
        yield tuple(pending[:-1]), pending[-1], event
        pending.clear()
    if pending:
        raise ValueError("Transaction ended with unmatched OrderFilled events")


def decode_and_normalize_leaf(
    raw_logs: Sequence[dict[str, Any]],
    *,
    rpc: PolygonRPC,
    manifest: PolygonMarketManifest,
    scan_id: str,
    exchange_address: str,
    from_block: int,
    to_block: int,
    ingested_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], int, str]:
    """Decode one successful RPC leaf and return rows, scoped count, scoped hash."""
    events = [decode_settlement_log(raw) for raw in raw_logs]
    expected_address = exchange_address.casefold()
    if any(
        event.exchange_address != expected_address
        or not from_block <= event.block_number <= to_block
        for event in events
    ):
        raise ValueError("Polygon provider returned a log outside the requested scope")
    locations = {(event.transaction_hash, event.log_index) for event in events}
    if len(locations) != len(events):
        raise ValueError("Polygon provider returned duplicate settlement logs")
    events.sort(
        key=lambda event: (
            event.block_number,
            event.transaction_index,
            event.log_index,
        )
    )
    token_ids = set(manifest.by_token)
    scoped_events = [event for event in events if event.token_id in token_ids]
    scoped_hash = _sha256_json([_event_payload(event) for event in scoped_events])
    timestamp = ingested_at or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    by_transaction: dict[str, list[DecodedSettlementEvent]] = defaultdict(list)
    for event in events:
        by_transaction[event.transaction_hash].append(event)
    ordered_transactions = sorted(
        (
            transaction
            for transaction in by_transaction.values()
            if any(event.token_id in token_ids for event in transaction)
        ),
        key=lambda tx_events: (
            tx_events[0].block_number,
            tx_events[0].transaction_index,
        ),
    )
    block_numbers = {
        event.block_number
        for transaction in ordered_transactions
        for event in transaction
    }
    blocks = _block_headers(rpc, block_numbers)
    for transaction in ordered_transactions:
        for event in transaction:
            block = blocks[event.block_number]
            if block.hash != event.block_hash:
                raise ValueError(
                    "Polygon log block hash disagrees with finalized header"
                )
    for transaction in ordered_transactions:
        for passive, active, matched in _transaction_segments(transaction):
            rows.extend(
                normalize_v2_segment(
                    passive,
                    active,
                    matched,
                    manifest=manifest,
                    scan_id=scan_id,
                    from_block=from_block,
                    to_block=to_block,
                    block_timestamp=blocks[active.block_number].timestamp,
                    ingested_at=timestamp,
                )
            )
    return rows, len(scoped_events), scoped_hash


def discover_and_normalize_leaf(
    raw_matched_logs: Sequence[dict[str, Any]],
    *,
    rpc: PolygonRPC,
    manifest: PolygonMarketManifest,
    scan_id: str,
    exchange_address: str,
    from_block: int,
    to_block: int,
    ingested_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], int, str, int]:
    """Expand target OrdersMatched discoveries through complete receipts.

    Pinned V2 ``_validateTokenIds`` constrains every passive order to the
    active order's condition, and ``OrdersMatched`` repeats that active token.
    Since the seed contains both positions, every target segment is therefore
    discoverable from its active aggregate without downloading unrelated
    ``OrderFilled`` logs for every Polymarket transaction.
    """
    expected_address = exchange_address.casefold()
    discoveries = [decode_settlement_log(raw) for raw in raw_matched_logs]
    if any(
        event.kind != "orders_matched"
        or event.exchange_address != expected_address
        or not from_block <= event.block_number <= to_block
        for event in discoveries
    ):
        raise ValueError("Polygon provider returned an invalid discovery log")
    discovery_locations = {
        (event.transaction_hash, event.log_index) for event in discoveries
    }
    if len(discovery_locations) != len(discoveries):
        raise ValueError("Polygon provider returned duplicate discovery logs")
    target_tokens = set(manifest.by_token)
    target_discoveries = sorted(
        (event for event in discoveries if event.token_id in target_tokens),
        key=lambda event: (
            event.block_number,
            event.transaction_index,
            event.log_index,
        ),
    )
    if not target_discoveries:
        return [], 0, _sha256_json([]), len(raw_matched_logs)

    receipts = rpc.transaction_receipts(
        event.transaction_hash for event in target_discoveries
    )
    receipt_logs: list[dict[str, Any]] = []
    for transaction_hash in dict.fromkeys(
        event.transaction_hash for event in target_discoveries
    ):
        receipt = receipts.get(transaction_hash)
        if receipt is None or not from_block <= receipt.block_number <= to_block:
            raise ValueError("Target Polygon discovery has no in-range receipt")
        for raw in receipt.logs:
            topics = raw.get("topics")
            if (
                str(raw.get("address", "")).casefold() == expected_address
                and isinstance(topics, list)
                and topics
                and str(topics[0]).casefold() in EVENT_TOPICS
            ):
                receipt_logs.append(raw)

    receipt_events = [decode_settlement_log(raw) for raw in receipt_logs]
    reconstructed_discoveries = sorted(
        (
            event
            for event in receipt_events
            if event.kind == "orders_matched" and event.token_id in target_tokens
        ),
        key=lambda event: (
            event.block_number,
            event.transaction_index,
            event.log_index,
        ),
    )
    if [_event_payload(event) for event in reconstructed_discoveries] != [
        _event_payload(event) for event in target_discoveries
    ]:
        raise ValueError("Polygon discovery and receipt logs disagree")

    _block_headers(rpc, (event.block_number for event in receipt_events))
    rows, scoped_count, scoped_hash = decode_and_normalize_leaf(
        receipt_logs,
        rpc=rpc,
        manifest=manifest,
        scan_id=scan_id,
        exchange_address=exchange_address,
        from_block=from_block,
        to_block=to_block,
        ingested_at=ingested_at,
    )
    return (
        rows,
        scoped_count,
        scoped_hash,
        len(raw_matched_logs) + len(receipt_logs),
    )


def _incremental_scoped_hash(
    transactions: Sequence[Sequence[DecodedSettlementEvent]],
    token_ids: set[str],
) -> tuple[int, str]:
    digest = hashlib.sha256()
    digest.update(b"[")
    first = True
    count = 0
    for transaction in transactions:
        for event in transaction:
            if event.token_id not in token_ids:
                continue
            if not first:
                digest.update(b",")
            digest.update(
                json.dumps(
                    _event_payload(event),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            first = False
            count += 1
    digest.update(b"]")
    return count, digest.hexdigest()


def _eligible_discovery(
    event: DecodedSettlementEvent,
    token_targets: Mapping[str, PolygonTokenTarget],
    exchange_address: str,
) -> bool:
    target = token_targets.get(event.token_id)
    return bool(
        target
        and target.exchange_address == exchange_address
        and target.first_valid_block <= event.block_number < target.first_invalid_block
    )


def _fetch_receipts_adaptively(
    rpc: PolygonRPC,
    transaction_hashes: Sequence[str],
    *,
    initial_batch_size: int,
) -> tuple[dict[str, Any], int]:
    receipts: dict[str, Any] = {}
    offset = 0
    batch_size = initial_batch_size
    adaptive_splits = 0
    while offset < len(transaction_hashes):
        batch = transaction_hashes[offset : offset + batch_size]
        before = getattr(rpc, "metrics", PolygonRPCMetrics()).copy()
        started = monotonic()
        try:
            batch_fetch = getattr(rpc, "transaction_receipt_batch", None)
            fetched = (
                batch_fetch(batch)
                if callable(batch_fetch)
                else rpc.transaction_receipts(batch)
            )
        except PolygonRPCSizeLimitError:
            if len(batch) <= _MIN_RECEIPT_BATCH_SIZE:
                raise RuntimeError(
                    "Polygon receipt batch exceeded the provider limit at the safe minimum"
                ) from None
            batch_size = max(_MIN_RECEIPT_BATCH_SIZE, batch_size // 2)
            adaptive_splits += 1
            continue
        duration = monotonic() - started
        after = getattr(rpc, "metrics", PolygonRPCMetrics())
        retried = after.retry_count > before.retry_count
        if set(fetched) != set(batch):
            raise ValueError("Polygon receipt batch did not return every transaction")
        receipts.update(fetched)
        offset += len(batch)
        if duration < 5 and not retried:
            batch_size = min(_MAX_RECEIPT_BATCH_SIZE, batch_size * 2)
        elif duration > 20 or retried:
            batch_size = max(_MIN_RECEIPT_BATCH_SIZE, batch_size // 2)
    return receipts, adaptive_splits


def _collect_and_normalize_leaf(
    *,
    rpc: PolygonRPC,
    manifest: PolygonMarketManifest,
    token_targets: Mapping[str, PolygonTokenTarget],
    token_index: Mapping[str, tuple[PolygonMarket, str]],
    scan_id: str,
    exchange_address: str,
    from_block: int,
    to_block: int,
    log_chunk_size: int,
    receipt_batch_size: int,
    adaptive_split_count: int = 0,
) -> PolygonLeafResult:
    started = monotonic()
    metrics_before = getattr(rpc, "metrics", PolygonRPCMetrics()).copy()
    log_started = monotonic()
    raw_discoveries = rpc.logs(
        exchange_address,
        from_block,
        to_block,
        event_topics=(ORDERS_MATCHED_TOPIC,),
    )
    log_duration = monotonic() - log_started
    discoveries = [decode_settlement_log(raw) for raw in raw_discoveries]
    if any(
        event.kind != "orders_matched"
        or event.exchange_address != exchange_address
        or not from_block <= event.block_number <= to_block
        for event in discoveries
    ):
        raise ValueError("Polygon provider returned an invalid discovery log")
    discovery_locations = {
        (event.transaction_hash, event.log_index) for event in discoveries
    }
    if len(discovery_locations) != len(discoveries):
        raise ValueError("Polygon provider returned duplicate discovery logs")
    eligible = sorted(
        (
            event
            for event in discoveries
            if _eligible_discovery(event, token_targets, exchange_address)
        ),
        key=lambda event: (
            event.block_number,
            event.transaction_index,
            event.log_index,
        ),
    )
    transaction_hashes = tuple(
        dict.fromkeys(event.transaction_hash for event in eligible)
    )
    receipts, receipt_splits = _fetch_receipts_adaptively(
        rpc,
        transaction_hashes,
        initial_batch_size=receipt_batch_size,
    )
    by_transaction: list[list[DecodedSettlementEvent]] = []
    receipt_log_count = 0
    for transaction_hash in transaction_hashes:
        receipt = receipts[transaction_hash]
        if not from_block <= receipt.block_number <= to_block:
            raise ValueError("Target Polygon discovery has no in-range receipt")
        raw_events = []
        for raw in receipt.logs:
            topics = raw.get("topics")
            if (
                str(raw.get("address", "")).casefold() == exchange_address
                and isinstance(topics, list)
                and topics
                and str(topics[0]).casefold() in EVENT_TOPICS
            ):
                raw_events.append(raw)
        receipt_log_count += len(raw_events)
        events = [decode_settlement_log(raw) for raw in raw_events]
        locations = {(event.transaction_hash, event.log_index) for event in events}
        if len(locations) != len(events):
            raise ValueError("Polygon receipt contains duplicate settlement logs")
        events.sort(key=lambda event: event.log_index)
        by_transaction.append(events)

    reconstructed = sorted(
        (
            event
            for transaction in by_transaction
            for event in transaction
            if event.kind == "orders_matched"
            and _eligible_discovery(event, token_targets, exchange_address)
        ),
        key=lambda event: (
            event.block_number,
            event.transaction_index,
            event.log_index,
        ),
    )
    if [_event_payload(event) for event in reconstructed] != [
        _event_payload(event) for event in eligible
    ]:
        raise ValueError("Polygon discovery and receipt logs disagree")

    by_transaction.sort(
        key=lambda transaction: (
            (
                transaction[0].block_number,
                transaction[0].transaction_index,
            )
            if transaction
            else (0, 0)
        )
    )
    block_numbers = {
        event.block_number for transaction in by_transaction for event in transaction
    }
    headers = _block_headers(rpc, (from_block, to_block, *sorted(block_numbers)))
    for transaction in by_transaction:
        for event in transaction:
            if headers[event.block_number].hash != event.block_hash:
                raise ValueError(
                    "Polygon log block hash disagrees with finalized header"
                )

    ingested_at = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    for transaction in by_transaction:
        for passive, active, matched in _transaction_segments(transaction):
            rows.extend(
                normalize_v2_segment(
                    passive,
                    active,
                    matched,
                    manifest=manifest,
                    scan_id=scan_id,
                    from_block=from_block,
                    to_block=to_block,
                    block_timestamp=headers[active.block_number].timestamp,
                    ingested_at=ingested_at,
                    token_index=token_index,
                )
            )
    scoped_count, scoped_hash = _incremental_scoped_hash(
        by_transaction, set(token_index)
    )
    if log_duration < 5 and len(discoveries) < 1_000:
        next_log_size = min(_MAX_LOG_CHUNK_SIZE, log_chunk_size * 2)
    elif log_duration > 30 or len(discoveries) > 10_000:
        next_log_size = max(_MIN_LOG_CHUNK_SIZE, log_chunk_size // 2)
    else:
        next_log_size = log_chunk_size
    rpc_metrics = getattr(rpc, "metrics", PolygonRPCMetrics()).delta(metrics_before)
    return PolygonLeafResult(
        exchange_address=exchange_address,
        from_block=from_block,
        to_block=to_block,
        from_block_hash=headers[from_block].hash,
        to_block_hash=headers[to_block].hash,
        rows=tuple(rows),
        scoped_event_count=scoped_count,
        scoped_event_sha256=scoped_hash,
        event_count=len(discoveries) + receipt_log_count,
        metrics=PolygonChunkMetrics(
            duration_ms=max(0, round((monotonic() - started) * 1_000)),
            http_request_count=rpc_metrics.http_request_count,
            log_rpc_call_count=rpc_metrics.log_rpc_call_count,
            receipt_rpc_call_count=rpc_metrics.receipt_rpc_call_count,
            header_rpc_call_count=rpc_metrics.header_rpc_call_count,
            discovery_count=len(discoveries),
            eligible_discovery_count=len(eligible),
            filtered_discovery_count=len(discoveries) - len(eligible),
            receipt_transaction_count=len(transaction_hashes),
            receipt_log_count=receipt_log_count,
            retry_count=rpc_metrics.retry_count,
            adaptive_split_count=adaptive_split_count + receipt_splits,
        ),
        next_log_chunk_size=next_log_size,
    )


def _collect_parent_range(
    *,
    rpc: PolygonRPC,
    manifest: PolygonMarketManifest,
    token_targets: Mapping[str, PolygonTokenTarget],
    token_index: Mapping[str, tuple[PolygonMarket, str]],
    scan_id: str,
    exchange_address: str,
    from_block: int,
    to_block: int,
    log_chunk_size: int,
    receipt_batch_size: int,
    adaptive_split_count: int = 0,
) -> tuple[list[PolygonLeafResult], Exception | None]:
    try:
        return [
            _collect_and_normalize_leaf(
                rpc=rpc,
                manifest=manifest,
                token_targets=token_targets,
                token_index=token_index,
                scan_id=scan_id,
                exchange_address=exchange_address,
                from_block=from_block,
                to_block=to_block,
                log_chunk_size=log_chunk_size,
                receipt_batch_size=receipt_batch_size,
                adaptive_split_count=adaptive_split_count,
            )
        ], None
    except PolygonRPCSizeLimitError as exc:
        if to_block - from_block + 1 <= _MIN_LOG_CHUNK_SIZE:
            return [], exc
        middle = (from_block + to_block) // 2
        split_size = max(_MIN_LOG_CHUNK_SIZE, log_chunk_size // 2)
        left, error = _collect_parent_range(
            rpc=rpc,
            manifest=manifest,
            token_targets=token_targets,
            token_index=token_index,
            scan_id=scan_id,
            exchange_address=exchange_address,
            from_block=from_block,
            to_block=middle,
            log_chunk_size=split_size,
            receipt_batch_size=receipt_batch_size,
            adaptive_split_count=adaptive_split_count + 1,
        )
        if error is not None:
            return left, error
        right, error = _collect_parent_range(
            rpc=rpc,
            manifest=manifest,
            token_targets=token_targets,
            token_index=token_index,
            scan_id=scan_id,
            exchange_address=exchange_address,
            from_block=middle + 1,
            to_block=to_block,
            log_chunk_size=split_size,
            receipt_batch_size=receipt_batch_size,
            adaptive_split_count=adaptive_split_count + 1,
        )
        return [*left, *right], error
    except Exception as exc:
        return [], exc


def _scan_id(
    manifest: PolygonMarketManifest,
    ranges: Sequence[PolygonTargetRange],
) -> tuple[str, str]:
    target = [item.as_dict() for item in ranges]
    boundary_hash = _sha256_json(target)
    value = {
        "manifest_sha256": manifest.sha256,
        "normalizer_version": NORMALIZER_VERSION,
        "chain_id": POLYGON_CHAIN_ID,
        "exchange_addresses": EXCHANGE_ADDRESSES,
        "target_ranges": target,
        "boundary_blocks_sha256": boundary_hash,
    }
    return _sha256_json(value), boundary_hash


def _parse_target_ranges(raw_ranges: Any) -> tuple[PolygonTargetRange, ...]:
    try:
        payload = json.loads(str(raw_ranges))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Stored Polygon target ranges are malformed") from exc
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("Stored Polygon target ranges are malformed")
    required_keys = {
        "exchange_address",
        "from_block",
        "to_block",
        "from_block_hash",
        "to_block_hash",
    }
    previous_end = defaultdict(lambda: -1)
    ranges: list[PolygonTargetRange] = []
    for item in payload:
        if not isinstance(item, dict) or set(item) != required_keys:
            raise RuntimeError("Stored Polygon target ranges are malformed")
        address = str(item["exchange_address"]).casefold()
        from_block = item["from_block"]
        to_block = item["to_block"]
        hashes = (item["from_block_hash"], item["to_block_hash"])
        if (
            address not in EXCHANGE_ADDRESSES
            or not isinstance(from_block, int)
            or isinstance(from_block, bool)
            or not isinstance(to_block, int)
            or isinstance(to_block, bool)
            or from_block < 0
            or to_block < from_block
            or from_block <= previous_end[address]
            or any(
                not isinstance(value, str)
                or not value.startswith("0x")
                or len(value) != 66
                for value in hashes
            )
        ):
            raise RuntimeError("Stored Polygon target ranges are malformed")
        try:
            for value in hashes:
                int(value[2:], 16)
        except ValueError as exc:
            raise RuntimeError("Stored Polygon target ranges are malformed") from exc
        ranges.append(
            PolygonTargetRange(
                exchange_address=address,
                from_block=from_block,
                to_block=to_block,
                from_block_hash=hashes[0].casefold(),
                to_block_hash=hashes[1].casefold(),
            )
        )
        previous_end[address] = to_block
    if {item.exchange_address for item in ranges} != set(EXCHANGE_ADDRESSES):
        raise RuntimeError("Stored Polygon target ranges are malformed")
    return tuple(ranges)


def _load_compatible_target_ranges(
    conn: duckdb.DuckDBPyConnection,
    rpc: PolygonRPC,
    manifest: PolygonMarketManifest,
    *,
    provider_label: str,
    provider_origin: str,
    finalized_head: PolygonBlock,
) -> tuple[PolygonTargetRange, ...] | None:
    """Reuse immutable finalized boundaries for a compatible interrupted scan."""
    row = conn.execute(
        f"""
        SELECT scan_id, target_ranges_json, boundary_blocks_sha256,
               finalized_head_number, finalized_head_hash
        FROM {RUNS_TABLE}
        WHERE manifest_version = ? AND manifest_sha256 = ?
          AND normalizer_version = ? AND chain_id = ?
          AND provider_label = ? AND provider_origin = ?
        ORDER BY started_at DESC
        LIMIT 1
        """,
        [
            manifest.version,
            manifest.sha256,
            NORMALIZER_VERSION,
            POLYGON_CHAIN_ID,
            provider_label,
            provider_origin,
        ],
    ).fetchone()
    if row is None:
        return None

    (
        stored_scan_id,
        raw_ranges,
        stored_boundary_hash,
        stored_head_number,
        stored_head_hash,
    ) = row
    candidate = _parse_target_ranges(raw_ranges)
    candidate_scan_id, candidate_boundary_hash = _scan_id(manifest, candidate)
    if (candidate_scan_id, candidate_boundary_hash) != (
        str(stored_scan_id),
        str(stored_boundary_hash),
    ):
        raise RuntimeError("Stored Polygon target-range provenance is inconsistent")

    stored_head_number = int(stored_head_number)
    if stored_head_number > finalized_head.number or any(
        target.to_block > finalized_head.number for target in candidate
    ):
        return None
    boundary_numbers = [
        number
        for target in candidate
        for number in (target.from_block, target.to_block)
    ]
    headers = _block_headers(rpc, [stored_head_number, *boundary_numbers])
    if headers[stored_head_number].hash != str(stored_head_hash):
        return None
    for target in candidate:
        if (
            headers[target.from_block].hash != target.from_block_hash
            or headers[target.to_block].hash != target.to_block_hash
        ):
            return None
    return candidate


def _gaps(
    target: tuple[int, int], completed: Sequence[tuple[int, int]]
) -> list[tuple[int, int]]:
    start, end = target
    cursor = start
    gaps: list[tuple[int, int]] = []
    coalesced: list[tuple[int, int]] = []
    for done_start, done_end in sorted(completed):
        if coalesced and done_start <= coalesced[-1][1]:
            raise RuntimeError("Completed Polygon chunks overlap")
        if coalesced and done_start == coalesced[-1][1] + 1:
            coalesced[-1] = (coalesced[-1][0], done_end)
        else:
            coalesced.append((done_start, done_end))
    for done_start, done_end in coalesced:
        if done_end < start or done_start > end:
            continue
        if done_start < cursor:
            raise RuntimeError(
                "Completed Polygon chunks overlap or cross target bounds"
            )
        if done_start > cursor:
            gaps.append((cursor, done_start - 1))
        cursor = done_end + 1
    if cursor <= end:
        gaps.append((cursor, end))
    return gaps


def _concurrent_leaf_results(
    work: Sequence[_RangeWork],
    *,
    rpc_factory: Any,
    manifest: PolygonMarketManifest,
    token_targets: Mapping[str, PolygonTokenTarget],
    token_index: Mapping[str, tuple[PolygonMarket, str]],
    scan_id: str,
    receipt_batch_size: int,
    workers: int,
) -> Iterator[tuple[_RangeWork, int, int, list[PolygonLeafResult], Exception | None]]:
    """Run one complete bounded leaf per disjoint target range at a time."""
    if workers <= 0:
        raise ValueError("Polygon workers must be positive")
    worker_state = local()
    ready = deque(work)
    pool = ThreadPoolExecutor(max_workers=workers)
    pending: dict[Future[Any], tuple[int, _RangeWork, int, int]] = {}
    ordinal = 0

    def collect(
        state: _RangeWork, start: int, end: int
    ) -> tuple[list[PolygonLeafResult], Exception | None]:
        worker_rpc = getattr(worker_state, "rpc", None)
        if worker_rpc is None:
            worker_rpc = rpc_factory()
            worker_state.rpc = worker_rpc
        return _collect_parent_range(
            rpc=worker_rpc,
            manifest=manifest,
            token_targets=token_targets,
            token_index=token_index,
            scan_id=scan_id,
            exchange_address=state.target.exchange_address,
            from_block=start,
            to_block=end,
            log_chunk_size=state.chunk_size,
            receipt_batch_size=receipt_batch_size,
        )

    def submit_ready() -> None:
        nonlocal ordinal
        while ready and len(pending) < workers:
            state = ready.popleft()
            chunk = state.next_chunk()
            if chunk is None:
                continue
            start, end = chunk
            future = pool.submit(collect, state, start, end)
            pending[future] = (ordinal, state, start, end)
            ordinal += 1

    try:
        submit_ready()
        while pending:
            completed, _ = wait(tuple(pending), return_when=FIRST_COMPLETED)
            for future in sorted(completed, key=lambda item: pending[item][0]):
                _ordinal, state, start, end = pending.pop(future)
                try:
                    leaves, error = future.result()
                except Exception as exc:  # defensive worker boundary
                    leaves, error = [], exc
                yield state, start, end, leaves, error
                if error is None:
                    if leaves:
                        state.chunk_size = leaves[-1].next_log_chunk_size
                    ready.append(state)
            submit_ready()
    finally:
        for future in pending:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)


def _revalidate_resumed_chunk_headers(
    conn: duckdb.DuckDBPyConnection,
    rpc: PolygonRPC,
    scan_id: str,
) -> dict[str, list[tuple[int, int]]]:
    rows = conn.execute(
        f"""
        SELECT exchange_address, from_block, to_block,
               from_block_hash, to_block_hash
        FROM {CHUNKS_TABLE}
        WHERE scan_id = ? AND status = 'success'
        ORDER BY exchange_address, from_block, to_block
        """,
        [scan_id],
    ).fetchall()
    headers = _block_headers(
        rpc,
        (
            number
            for _address, from_block, to_block, _from_hash, _to_hash in rows
            for number in (int(from_block), int(to_block))
        ),
    )
    completed: dict[str, list[tuple[int, int]]] = defaultdict(list)
    stale: list[tuple[str, int, int]] = []
    for address, from_block, to_block, from_hash, to_hash in rows:
        start = int(from_block)
        end = int(to_block)
        if (headers[start].hash, headers[end].hash) != (
            str(from_hash),
            str(to_hash),
        ):
            stale.append((str(address), start, end))
        else:
            completed[str(address)].append((start, end))
    if not stale:
        return dict(completed)

    run_state = conn.execute(
        f"SELECT status, raw_published FROM {RUNS_TABLE} WHERE scan_id = ?",
        [scan_id],
    ).fetchone()
    if run_state and str(run_state[0]) == "published" and bool(run_state[1]):
        return completed_polygon_chunk_ranges(conn, scan_id)

    conn.execute("BEGIN TRANSACTION")
    try:
        for address, start, end in stale:
            conn.execute(
                f"""
                DELETE FROM {STAGE_TABLE}
                WHERE scan_id = ? AND exchange_address = ?
                  AND chunk_from_block = ? AND chunk_to_block = ?
                """,
                [scan_id, address, start, end],
            )
            conn.execute(
                f"""
                DELETE FROM {CHUNKS_TABLE}
                WHERE scan_id = ? AND exchange_address = ?
                  AND from_block = ? AND to_block = ? AND status = 'success'
                """,
                [scan_id, address, start, end],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    raise RuntimeError(
        "Stored Polygon leaf boundary hash changed; stale leaves were discarded"
    )


def _offline_published_summary(
    conn: duckdb.DuckDBPyConnection,
    manifest: PolygonMarketManifest,
) -> dict[str, Any] | None:
    row = conn.execute(
        f"""
        SELECT scan_id, target_ranges_json, boundary_blocks_sha256,
               finalized_head_number, finalized_head_hash
        FROM {RUNS_TABLE}
        WHERE manifest_version = ? AND manifest_sha256 = ?
          AND normalizer_version = ? AND chain_id = ?
          AND status = 'published' AND raw_published = TRUE
        ORDER BY published_at DESC
        LIMIT 1
        """,
        [manifest.version, manifest.sha256, NORMALIZER_VERSION, POLYGON_CHAIN_ID],
    ).fetchone()
    if row is None:
        return None
    scan_id, raw_ranges, boundary_hash, head_number, head_hash = row
    ranges = _parse_target_ranges(raw_ranges)
    expected_scan_id, expected_boundary_hash = _scan_id(manifest, ranges)
    if (str(scan_id), str(boundary_hash)) != (
        expected_scan_id,
        expected_boundary_hash,
    ):
        raise RuntimeError("Published Polygon scan provenance is inconsistent")
    completed = completed_polygon_chunk_ranges(conn, str(scan_id))
    targets_by_address: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for target in ranges:
        targets_by_address[target.exchange_address].append(
            (target.from_block, target.to_block)
        )
    if set(completed) != set(targets_by_address):
        raise RuntimeError("Published Polygon scan has incomplete exchange coverage")
    for address, targets in targets_by_address.items():
        leaves = completed[address]
        used = 0
        for target_start, target_end in targets:
            cursor = target_start
            while used < len(leaves) and leaves[used][0] <= target_end:
                leaf_start, leaf_end = leaves[used]
                if leaf_start != cursor or leaf_end > target_end:
                    raise RuntimeError("Published Polygon scan has a gap or overlap")
                cursor = leaf_end + 1
                used += 1
            if cursor != target_end + 1:
                raise RuntimeError("Published Polygon scan has incomplete coverage")
        if used != len(leaves):
            raise RuntimeError("Published Polygon scan extends outside target ranges")
    canonical = conn.execute(
        f"""
        SELECT count(*), count(distinct scan_id)
        FROM {FILLS_TABLE}
        """
    ).fetchone()
    fill_count = int(canonical[0])
    expected_fill_count = int(
        conn.execute(
            f"""
            SELECT coalesce(sum(normalized_fill_count), 0)
            FROM {CHUNKS_TABLE}
            WHERE scan_id = ? AND status = 'success'
            """,
            [scan_id],
        ).fetchone()[0]
    )
    if (
        fill_count <= 0
        or int(canonical[1]) != 1
        or expected_fill_count != fill_count
        or int(
            conn.execute(
                f"SELECT count(*) FROM {FILLS_TABLE} WHERE scan_id = ?", [scan_id]
            ).fetchone()[0]
        )
        != fill_count
    ):
        raise RuntimeError("Published Polygon scan canonical fills are inconsistent")
    return {
        "scan_id": str(scan_id),
        "status": "published",
        "published": True,
        "short_circuited": True,
        "offline": True,
        "manifest_sha256": manifest.sha256,
        "manifest_version": manifest.version,
        "finalized_head_number": int(head_number),
        "finalized_head_hash": str(head_hash),
        "target_range_count": len(ranges),
        "completed_chunk_count": sum(len(value) for value in completed.values()),
        "resumed_chunk_count": 0,
        "scanned_chunk_count": 0,
        "fill_count": fill_count,
    }


_STATUS_FIELDS = frozenset(
    {
        "scan_id",
        "version",
        "status",
        "exchange_address",
        "from_block",
        "to_block",
        "target_blocks",
        "completed_blocks",
        "completed_percent",
        "successful_chunks",
        "active_workers",
        "queued_work",
        "event_count",
        "receipt_count",
        "fill_count",
        "rpc_count",
        "blocks_per_second",
        "events_per_second",
        "elapsed_seconds",
        "last_checkpoint_at_utc",
        "error_type",
    }
)


def _warehouse_status_path(conn: duckdb.DuckDBPyConnection, scan_id: str) -> Path:
    databases = conn.execute("PRAGMA database_list").fetchall()
    warehouse = next(
        (str(row[2]) for row in databases if len(row) > 2 and str(row[2]).strip()),
        "memory",
    )
    warehouse_key = hashlib.sha256(warehouse.encode("utf-8")).hexdigest()[:16]
    return _STATUS_ROOT / f"{warehouse_key}-{scan_id[:16]}.json"


def _write_status(path: Path, payload: Mapping[str, Any]) -> None:
    if set(payload) - _STATUS_FIELDS:
        raise ValueError("Polygon status payload contains a prohibited field")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _scan_status_totals(
    conn: duckdb.DuckDBPyConnection, scan_id: str
) -> tuple[int, int, int, int, int, int]:
    row = conn.execute(
        f"""
        SELECT count(*), coalesce(sum(to_block - from_block + 1), 0),
               coalesce(sum(event_count), 0),
               coalesce(sum(receipt_transaction_count), 0),
               coalesce(sum(normalized_fill_count), 0),
               coalesce(sum(http_request_count), 0)
        FROM {CHUNKS_TABLE}
        WHERE scan_id = ? AND status = 'success'
        """,
        [scan_id],
    ).fetchone()
    return tuple(int(value) for value in row)


def sync_polygon_settlement_fills(
    conn: duckdb.DuckDBPyConnection,
    *,
    seed_path: Path = DEFAULT_POLYGON_MARKET_SEED_PATH,
    rpc_url: str,
    provider_label: str,
    config: PolygonSettlementSyncConfig = PolygonSettlementSyncConfig(),
    client: PolygonRPC | None = None,
    log: Any = logger,
) -> dict[str, Any]:
    """Resume finalized leaf scans and atomically publish the complete snapshot."""
    manifest = load_polygon_market_seed(seed_path)
    offline = _offline_published_summary(conn, manifest)
    if offline is not None:
        return offline
    if not rpc_url.strip() or not provider_label.strip():
        raise ValueError("POLYGON_RPC_URL and POLYGON_RPC_PROVIDER_LABEL are required")
    provider_label = validate_polygon_provider_label(provider_label)
    rate_limiter = RateLimiter(config.requests_per_second) if client is None else None
    rpc = client or PolygonRPC(
        rpc_url,
        retries=config.transient_retries,
        backoff_factor=config.transient_backoff_seconds,
        requests_per_second=config.requests_per_second,
        rate_limiter=rate_limiter,
    )
    if rpc.chain_id() != POLYGON_CHAIN_ID:
        raise PolygonRPCError(f"Expected Polygon chain ID {POLYGON_CHAIN_ID}")
    finalized_head = rpc.finalized_head()
    plan = build_polygon_scan_plan(rpc, manifest, finalized_head)
    ranges = plan.target_ranges
    if not ranges:
        raise RuntimeError("Polygon manifest produced no target block ranges")
    scan_id, boundary_hash = _scan_id(manifest, ranges)
    target_ranges = [item.as_dict() for item in ranges]
    already_published = start_polygon_settlement_scan(
        conn,
        scan_id=scan_id,
        manifest_version=manifest.version,
        manifest_sha256=manifest.sha256,
        normalizer_version=NORMALIZER_VERSION,
        chain_id=POLYGON_CHAIN_ID,
        provider_label=provider_label,
        provider_origin=rpc.origin,
        finalized_head_number=finalized_head.number,
        finalized_head_hash=finalized_head.hash,
        target_ranges=target_ranges,
        boundary_blocks_sha256=boundary_hash,
    )
    if already_published:
        published = _offline_published_summary(conn, manifest)
        if published is None:
            raise RuntimeError("Published Polygon scan disappeared during startup")
        return published

    try:
        initially_completed = _revalidate_resumed_chunk_headers(conn, rpc, scan_id)
    except Exception as exc:
        record_polygon_settlement_failure(conn, scan_id=scan_id, error=exc)
        raise
    resumed = sum(len(value) for value in initially_completed.values())
    scanned = 0
    scan_started = monotonic()
    guardrail = ProgressGuardrail(
        asset="polymarket_wc2026_polygon_settlement_backfill",
        logger=log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=10,
    )
    current_address: str | None = None
    current_range: tuple[int, int] | None = None
    completed = completed_polygon_chunk_ranges(conn, scan_id)
    work = [
        _RangeWork(
            target=target,
            gaps=deque(
                _gaps(
                    (target.from_block, target.to_block),
                    completed.get(target.exchange_address, []),
                )
            ),
            chunk_size=config.initial_block_chunk_size,
        )
        for target in ranges
    ]
    work = [state for state in work if state.gaps]
    target_blocks = sum(target.to_block - target.from_block + 1 for target in ranges)
    status_path = _warehouse_status_path(conn, scan_id)

    def rpc_activity(method: str) -> None:
        guardrail.record_progress(
            phase="polygon_rpc_activity",
            diagnostics={"rpc_method": method},
        )

    def worker_rpc() -> PolygonRPC:
        if rate_limiter is None:
            raise RuntimeError("Concurrent Polygon workers require a shared limiter")
        return PolygonRPC(
            rpc_url,
            retries=config.transient_retries,
            backoff_factor=config.transient_backoff_seconds,
            requests_per_second=config.requests_per_second,
            rate_limiter=rate_limiter,
            activity_callback=rpc_activity,
        )

    results = _concurrent_leaf_results(
        work,
        rpc_factory=worker_rpc if client is None else lambda: client,
        manifest=manifest,
        token_targets=plan.token_targets,
        token_index={
            token_id: (target.market, target.outcome_side)
            for token_id, target in plan.token_targets.items()
        },
        scan_id=scan_id,
        receipt_batch_size=config.initial_receipt_batch_size,
        workers=config.workers if client is None else 1,
    )
    try:
        try:
            for state, chunk_start, chunk_end, leaves, terminal_error in results:
                current_address = state.target.exchange_address
                current_range = (chunk_start, chunk_end)
                guardrail.check(
                    phase="polygon_rpc",
                    diagnostics={
                        "exchange_address": current_address,
                        "from_block": chunk_start,
                        "to_block": chunk_end,
                    },
                )
                for leaf in leaves:
                    current_range = (leaf.from_block, leaf.to_block)
                    record_polygon_settlement_chunk(
                        conn,
                        scan_id=scan_id,
                        exchange_address=leaf.exchange_address,
                        from_block=leaf.from_block,
                        to_block=leaf.to_block,
                        from_block_hash=leaf.from_block_hash,
                        to_block_hash=leaf.to_block_hash,
                        event_count=leaf.event_count,
                        scoped_event_count=leaf.scoped_event_count,
                        scoped_event_sha256=leaf.scoped_event_sha256,
                        rows=leaf.rows,
                        metrics=leaf.metrics.as_dict(),
                    )
                    scanned += 1
                    guardrail.record_progress(
                        phase="polygon_chunk",
                        diagnostics={
                            "exchange_address": leaf.exchange_address,
                            "from_block": leaf.from_block,
                            "to_block": leaf.to_block,
                            "fills": len(leaf.rows),
                        },
                    )
                    totals = _scan_status_totals(conn, scan_id)
                    elapsed = max(monotonic() - scan_started, 0.001)
                    completed_blocks = int(totals[1])
                    unfinished = sum(
                        len(item.gaps) + int(item.cursor is not None) for item in work
                    )
                    _write_status(
                        status_path,
                        {
                            "scan_id": scan_id,
                            "version": NORMALIZER_VERSION,
                            "status": "running",
                            "exchange_address": leaf.exchange_address,
                            "from_block": leaf.from_block,
                            "to_block": leaf.to_block,
                            "target_blocks": target_blocks,
                            "completed_blocks": completed_blocks,
                            "completed_percent": round(
                                completed_blocks * 100 / target_blocks, 6
                            ),
                            "successful_chunks": int(totals[0]),
                            "active_workers": min(config.workers, unfinished),
                            "queued_work": unfinished,
                            "event_count": int(totals[2]),
                            "receipt_count": int(totals[3]),
                            "fill_count": int(totals[4]),
                            "rpc_count": int(totals[5]),
                            "blocks_per_second": round(completed_blocks / elapsed, 6),
                            "events_per_second": round(int(totals[2]) / elapsed, 6),
                            "elapsed_seconds": round(elapsed, 3),
                            "last_checkpoint_at_utc": datetime.now(timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z"),
                            "error_type": None,
                        },
                    )
                current_range = (chunk_start, chunk_end)
                if terminal_error is not None:
                    raise terminal_error
        finally:
            close = getattr(results, "close", None)
            if close is not None:
                close()
        fill_count = publish_polygon_settlement_scan(
            conn,
            scan_id=scan_id,
            target_ranges=target_ranges,
        )
    except Exception as exc:
        record_polygon_settlement_failure(
            conn,
            scan_id=scan_id,
            error=exc,
            exchange_address=current_address,
            from_block=current_range[0] if current_range else None,
            to_block=current_range[1] if current_range else None,
        )
        totals = _scan_status_totals(conn, scan_id)
        elapsed = max(monotonic() - scan_started, 0.001)
        _write_status(
            status_path,
            {
                "scan_id": scan_id,
                "version": NORMALIZER_VERSION,
                "status": "failed",
                "exchange_address": current_address,
                "from_block": current_range[0] if current_range else None,
                "to_block": current_range[1] if current_range else None,
                "target_blocks": target_blocks,
                "completed_blocks": totals[1],
                "completed_percent": round(totals[1] * 100 / target_blocks, 6),
                "successful_chunks": totals[0],
                "active_workers": 0,
                "queued_work": 0,
                "event_count": totals[2],
                "receipt_count": totals[3],
                "fill_count": totals[4],
                "rpc_count": totals[5],
                "blocks_per_second": round(totals[1] / elapsed, 6),
                "events_per_second": round(totals[2] / elapsed, 6),
                "elapsed_seconds": round(elapsed, 3),
                "last_checkpoint_at_utc": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "error_type": exc.__class__.__name__,
            },
        )
        raise

    totals = _scan_status_totals(conn, scan_id)
    completed_count = totals[0]
    elapsed = max(monotonic() - scan_started, 0.001)
    _write_status(
        status_path,
        {
            "scan_id": scan_id,
            "version": NORMALIZER_VERSION,
            "status": "published",
            "exchange_address": None,
            "from_block": None,
            "to_block": None,
            "target_blocks": target_blocks,
            "completed_blocks": target_blocks,
            "completed_percent": 100,
            "successful_chunks": completed_count,
            "active_workers": 0,
            "queued_work": 0,
            "event_count": totals[2],
            "receipt_count": totals[3],
            "fill_count": fill_count,
            "rpc_count": totals[5],
            "blocks_per_second": round(target_blocks / elapsed, 6),
            "events_per_second": round(totals[2] / elapsed, 6),
            "elapsed_seconds": round(elapsed, 3),
            "last_checkpoint_at_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "error_type": None,
        },
    )
    return {
        "scan_id": scan_id,
        "status": "published",
        "published": True,
        "short_circuited": False,
        "manifest_sha256": manifest.sha256,
        "manifest_version": manifest.version,
        "finalized_head_number": finalized_head.number,
        "finalized_head_hash": finalized_head.hash,
        "target_range_count": len(ranges),
        "completed_chunk_count": completed_count,
        "resumed_chunk_count": resumed,
        "scanned_chunk_count": scanned,
        "fill_count": fill_count,
    }


def verify_polygon_settlement_scan(
    conn: duckdb.DuckDBPyConnection,
    *,
    seed_path: Path = DEFAULT_POLYGON_MARKET_SEED_PATH,
    rpc_url: str,
    provider_label: str,
    client: PolygonRPC | None = None,
) -> dict[str, Any]:
    """Warning-only comparison of published leaf hashes with a second provider."""
    scan_rows = conn.execute(f"SELECT DISTINCT scan_id FROM {FILLS_TABLE}").fetchall()
    if len(scan_rows) != 1:
        raise RuntimeError("Expected one canonical Polygon scan before verification")
    scan_id = str(scan_rows[0][0])
    has_rpc_url = bool(rpc_url.strip())
    has_provider_label = bool(provider_label.strip())
    if not has_rpc_url and not has_provider_label:
        set_polygon_verification_status(conn, scan_id, "not_requested")
        return {"scan_id": scan_id, "verification_status": "not_requested"}
    if has_rpc_url != has_provider_label:
        set_polygon_verification_status(conn, scan_id, "error")
        return {
            "scan_id": scan_id,
            "verification_status": "error",
            "error_type": "VerificationConfigurationError",
        }
    provider_label = validate_polygon_provider_label(
        provider_label,
        field="verification provider_label",
    )
    primary_provider = conn.execute(
        f"""
        SELECT provider_label, provider_origin
        FROM {RUNS_TABLE}
        WHERE scan_id = ? AND status = 'published' AND raw_published = TRUE
        """,
        [scan_id],
    ).fetchone()
    if primary_provider is None:
        raise RuntimeError("Canonical Polygon settlement scan is not published")
    rpc = client or PolygonRPC(rpc_url)
    same_label = (
        provider_label.strip().casefold() == str(primary_provider[0]).strip().casefold()
    )
    same_origin = rpc.origin.casefold() == str(primary_provider[1]).casefold()
    if same_label or same_origin:
        set_polygon_verification_status(
            conn,
            scan_id,
            "error",
            provider_label=provider_label,
            provider_origin=rpc.origin,
        )
        return {
            "scan_id": scan_id,
            "verification_status": "error",
            "error_type": "NonIndependentVerificationProvider",
        }
    manifest = load_polygon_market_seed(seed_path)
    mismatches: list[dict[str, Any]] = []
    try:
        if rpc.chain_id() != POLYGON_CHAIN_ID:
            raise PolygonRPCError(f"Expected Polygon chain ID {POLYGON_CHAIN_ID}")
        chunks = conn.execute(
            f"""
            SELECT exchange_address, from_block, to_block, from_block_hash,
                   to_block_hash, scoped_event_sha256
            FROM {CHUNKS_TABLE}
            WHERE scan_id = ? AND status = 'success'
            ORDER BY exchange_address, from_block, to_block
            """,
            [scan_id],
        ).fetchall()
        headers = _block_headers(
            rpc,
            (
                number
                for _address, from_block, to_block, *_rest in chunks
                for number in (int(from_block), int(to_block))
            ),
        )
        for (
            address,
            from_block,
            to_block,
            start_hash,
            end_hash,
            expected_hash,
        ) in chunks:
            actual_start = headers[int(from_block)].hash
            actual_end = headers[int(to_block)].hash
            raw_logs = [
                raw
                for _, _, leaf in adaptive_log_leaves(
                    rpc,
                    str(address),
                    int(from_block),
                    int(to_block),
                    event_topics=(ORDERS_MATCHED_TOPIC,)
                    if client is None
                    else EVENT_TOPICS,
                )
                for raw in leaf
            ]
            if client is None:
                _, _, actual_hash, _ = discover_and_normalize_leaf(
                    raw_logs,
                    rpc=rpc,
                    manifest=manifest,
                    scan_id=scan_id,
                    exchange_address=str(address),
                    from_block=int(from_block),
                    to_block=int(to_block),
                )
            else:
                _, _, actual_hash = decode_and_normalize_leaf(
                    raw_logs,
                    rpc=rpc,
                    manifest=manifest,
                    scan_id=scan_id,
                    exchange_address=str(address),
                    from_block=int(from_block),
                    to_block=int(to_block),
                )
            if (actual_start, actual_end, actual_hash) != (
                start_hash,
                end_hash,
                expected_hash,
            ):
                mismatches.append(
                    {
                        "exchange_address": address,
                        "from_block": int(from_block),
                        "to_block": int(to_block),
                    }
                )
        status = "mismatched" if mismatches else "matched"
    except Exception as exc:
        set_polygon_verification_status(
            conn,
            scan_id,
            "error",
            provider_label=provider_label,
            provider_origin=rpc.origin,
        )
        return {
            "scan_id": scan_id,
            "verification_status": "error",
            "error_type": exc.__class__.__name__,
        }
    set_polygon_verification_status(
        conn,
        scan_id,
        status,
        provider_label=provider_label,
        provider_origin=rpc.origin,
    )
    return {
        "scan_id": scan_id,
        "verification_status": status,
        "mismatched_chunks": mismatches,
    }


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
