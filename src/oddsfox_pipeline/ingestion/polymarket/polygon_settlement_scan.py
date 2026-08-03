"""Polygon settlement scan planning and concurrent collection."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict, deque
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from threading import local
from time import monotonic
from typing import Any, Iterable

import duckdb

from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import (
    EVENT_TOPICS,
    ORDERS_MATCHED_TOPIC,
    DecodedSettlementEvent,
    PolygonBlock,
    PolygonRPC,
    PolygonRPCMetrics,
    PolygonRPCSizeLimitError,
    decode_settlement_log,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    POLYGON_CHAIN_ID,
    PolygonMarket,
    PolygonMarketManifest,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_normalize import (
    _event_payload,
    _sha256_json,
    _transaction_segments,
    normalize_v2_segment,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_types import (
    _MAX_LOG_CHUNK_SIZE,
    _MAX_RECEIPT_BATCH_SIZE,
    _MIN_LOG_CHUNK_SIZE,
    _MIN_RECEIPT_BATCH_SIZE,
    EXCHANGE_ADDRESSES,
    NORMALIZER_VERSION,
    PolygonChunkMetrics,
    PolygonLeafResult,
    PolygonScanPlan,
    PolygonTargetRange,
    PolygonTokenTarget,
    _RangeWork,
)
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (
    CHUNKS_TABLE,
    RUNS_TABLE,
    STAGE_TABLE,
    completed_polygon_chunk_ranges,
)

logger = logging.getLogger(__name__)


def _block_headers(rpc: PolygonRPC, numbers: Iterable[int]) -> dict[int, PolygonBlock]:
    """Use production batching while keeping injected replay clients minimal."""
    requested = tuple(dict.fromkeys(numbers))
    batch = getattr(rpc, "blocks", None)
    if callable(batch):
        return batch(requested)
    return {number: rpc.block(number) for number in requested}


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
