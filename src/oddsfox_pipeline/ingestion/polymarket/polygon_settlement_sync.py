"""Polygon settlement sync, verification, and status."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from collections import defaultdict, deque
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

import duckdb

from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import (
    EVENT_TOPICS,
    ORDERS_MATCHED_TOPIC,
    PolygonRPC,
    PolygonRPCError,
    adaptive_log_leaves,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    DEFAULT_POLYGON_MARKET_SEED_PATH,
    POLYGON_CHAIN_ID,
    PolygonMarketManifest,
    load_polygon_market_seed,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_normalize import (
    decode_and_normalize_leaf,
    discover_and_normalize_leaf,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_scan import (
    _block_headers,
    _concurrent_leaf_results,
    _gaps,
    _parse_target_ranges,
    _revalidate_resumed_chunk_headers,
    _scan_id,
    build_polygon_scan_plan,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement_types import (
    _STATUS_ROOT,
    NORMALIZER_VERSION,
    PolygonSettlementSyncConfig,
    _RangeWork,
)
from oddsfox_pipeline.resources.http import RateLimiter
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (
    CHUNKS_TABLE,
    FILLS_TABLE,
    RUNS_TABLE,
    completed_polygon_chunk_ranges,
    publish_polygon_settlement_scan,
    record_polygon_settlement_chunk,
    record_polygon_settlement_failure,
    set_polygon_verification_status,
    start_polygon_settlement_scan,
    validate_polygon_provider_label,
)

logger = logging.getLogger(__name__)


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
