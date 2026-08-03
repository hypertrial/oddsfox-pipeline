"""Polygon V2 settlement normalization and decoding."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import (
    EVENT_TOPICS,
    DecodedSettlementEvent,
    PolygonRPC,
    decode_settlement_log,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    POLYGON_CHAIN_ID,
    PolygonMarket,
    PolygonMarketManifest,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_types import (
    _MAX_RATIO_SAFE_VOLUME_UNSCALED,
    _PRICE_QUANTUM,
    _PRICE_SCALE,
    _VOLUME_QUANTUM,
    NORMALIZER_VERSION,
)


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
    from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_scan import (
        _block_headers,
    )

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
    from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_scan import (
        _block_headers,
    )

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
