"""Resumable Polygon settlement staging and atomic snapshot publication."""

from __future__ import annotations

import json
import re
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import duckdb
import pyarrow as pa

from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_wc2026_ops_tbl,
    polymarket_wc2026_raw_tbl,
)

FILLS_TABLE = polymarket_wc2026_raw_tbl("polygon_settlement_fills")
RUNS_TABLE = polymarket_wc2026_ops_tbl("polygon_settlement_scan_runs")
CHUNKS_TABLE = polymarket_wc2026_ops_tbl("polygon_settlement_scan_chunks")
STAGE_TABLE = polymarket_wc2026_ops_tbl("polygon_settlement_fill_stage")

_FIXED_V2_EXCHANGE_ADDRESSES = frozenset(
    {
        "0xe111180000d2663c0091e4f400237545b87b996b",
        "0xe2222d279d744050d28e00520010520000310f59",
    }
)

FILL_COLUMNS = (
    "scan_id",
    "chain_id",
    "exchange_address",
    "chunk_from_block",
    "chunk_to_block",
    "block_number",
    "block_hash",
    "block_timestamp",
    "transaction_hash",
    "transaction_index",
    "passive_log_index",
    "active_log_index",
    "matched_log_index",
    "normalized_leg_ordinal",
    "proposition_id",
    "condition_id",
    "token_id",
    "outcome_side",
    "order_side",
    "source_token_id",
    "source_maker_amount",
    "source_taker_amount",
    "share_volume",
    "gross_collateral_volume",
    "price",
    "normalization_kind",
    "is_derived",
    "segment_sha256",
    "decoder_version",
    "ingested_at",
)

_FILL_TIMESTAMP_COLUMNS = frozenset({"block_timestamp", "ingested_at"})
CHUNK_METRIC_COLUMNS = (
    "duration_ms",
    "http_request_count",
    "log_rpc_call_count",
    "receipt_rpc_call_count",
    "header_rpc_call_count",
    "discovery_count",
    "eligible_discovery_count",
    "filtered_discovery_count",
    "receipt_transaction_count",
    "receipt_log_count",
    "retry_count",
    "adaptive_split_count",
)

_FILL_ARROW_SCHEMA = pa.schema(
    [
        pa.field("scan_id", pa.string(), nullable=False),
        pa.field("chain_id", pa.int32(), nullable=False),
        pa.field("exchange_address", pa.string(), nullable=False),
        pa.field("chunk_from_block", pa.int64(), nullable=False),
        pa.field("chunk_to_block", pa.int64(), nullable=False),
        pa.field("block_number", pa.int64(), nullable=False),
        pa.field("block_hash", pa.string(), nullable=False),
        pa.field("block_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("transaction_hash", pa.string(), nullable=False),
        pa.field("transaction_index", pa.int64(), nullable=False),
        pa.field("passive_log_index", pa.int64(), nullable=False),
        pa.field("active_log_index", pa.int64(), nullable=False),
        pa.field("matched_log_index", pa.int64(), nullable=False),
        pa.field("normalized_leg_ordinal", pa.int16(), nullable=False),
        pa.field("proposition_id", pa.string(), nullable=False),
        pa.field("condition_id", pa.string(), nullable=False),
        pa.field("token_id", pa.string(), nullable=False),
        pa.field("outcome_side", pa.string(), nullable=False),
        pa.field("order_side", pa.string(), nullable=False),
        pa.field("source_token_id", pa.string(), nullable=False),
        pa.field("source_maker_amount", pa.string(), nullable=False),
        pa.field("source_taker_amount", pa.string(), nullable=False),
        pa.field("share_volume", pa.decimal128(38, 6), nullable=False),
        pa.field("gross_collateral_volume", pa.decimal128(38, 6), nullable=False),
        pa.field("price", pa.decimal128(38, 18), nullable=False),
        pa.field("normalization_kind", pa.string(), nullable=False),
        pa.field("is_derived", pa.bool_(), nullable=False),
        pa.field("segment_sha256", pa.string(), nullable=False),
        pa.field("decoder_version", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)


def _utc_naive(value: Any, *, field: str) -> datetime:
    """Bind an instant to DuckDB TIMESTAMP without applying the host timezone."""
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _fill_arrow_table(rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    normalized = [
        {
            column: (
                _utc_naive(row[column], field=column)
                if column in _FILL_TIMESTAMP_COLUMNS
                else row[column]
            )
            for column in FILL_COLUMNS
        }
        for row in rows
    ]
    return pa.Table.from_pylist(normalized, schema=_FILL_ARROW_SCHEMA)


def _clean_error(value: BaseException | str) -> str:
    if (
        isinstance(value, BaseException)
        and value.__class__.__name__ == "PolygonRPCError"
    ):
        return "Polygon RPC failure"
    text = re.sub(r"https?://\S+", "<redacted-url>", str(value))
    return " ".join(text.split())[:500]


def validate_polygon_provider_label(
    value: str, *, field: str = "provider_label"
) -> str:
    label = value.strip()
    if not re.fullmatch(
        r"[A-Za-z0-9](?:[A-Za-z0-9 _.-]{0,62}[A-Za-z0-9])?",
        label,
    ):
        raise ValueError(f"{field} must be a safe 1-64 character display label")
    return label


def _validated_provider_origin(value: str) -> str:
    origin = value.strip()
    parsed = urlsplit(origin)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("provider_origin must be a sanitized HTTPS origin") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("provider_origin must be a sanitized HTTPS origin")
    host = parsed.hostname.casefold()
    if ":" in host:
        host = f"[{host}]"
    canonical = f"https://{host}{'' if port in (None, 443) else f':{port}'}"
    if origin != canonical:
        raise ValueError("provider_origin must be a sanitized HTTPS origin")
    return origin


def _validated_target_ranges_json(
    target_ranges: Sequence[Mapping[str, Any]],
) -> str:
    ranges = list(target_ranges)
    try:
        addresses = {str(item["exchange_address"]).casefold() for item in ranges}
    except (KeyError, TypeError) as exc:
        raise ValueError("Polygon target ranges are malformed") from exc
    if addresses != _FIXED_V2_EXCHANGE_ADDRESSES:
        raise ValueError("Polygon target ranges must cover both fixed V2 exchanges")
    return json.dumps(ranges, sort_keys=True, separators=(",", ":"))


def start_polygon_settlement_scan(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    manifest_version: str,
    manifest_sha256: str,
    normalizer_version: str,
    chain_id: int,
    provider_label: str,
    provider_origin: str,
    finalized_head_number: int,
    finalized_head_hash: str,
    target_ranges: Sequence[Mapping[str, Any]],
    boundary_blocks_sha256: str,
) -> bool:
    """Start/resume a compatible scan; return True when already published."""
    label = validate_polygon_provider_label(provider_label)
    origin = _validated_provider_origin(provider_origin)
    ranges_json = _validated_target_ranges_json(target_ranges)
    expected = (
        manifest_version,
        manifest_sha256,
        normalizer_version,
        chain_id,
        label,
        origin,
        finalized_head_number,
        finalized_head_hash,
        ranges_json,
        boundary_blocks_sha256,
    )
    existing = conn.execute(
        f"""
        SELECT manifest_version, manifest_sha256, normalizer_version, chain_id,
               provider_label, provider_origin, finalized_head_number,
               finalized_head_hash, target_ranges_json, boundary_blocks_sha256,
               status, raw_published
        FROM {RUNS_TABLE}
        WHERE scan_id = ?
        """,
        [scan_id],
    ).fetchone()
    if existing:
        existing_identity = (*existing[:6], *existing[8:10])
        expected_identity = (*expected[:6], *expected[8:10])
        if existing_identity != expected_identity:
            raise RuntimeError(f"Scan {scan_id} exists with incompatible provenance")
        status, raw_published = str(existing[-2]), bool(existing[-1])
        if status == "published" and raw_published:
            canonical = conn.execute(
                f"SELECT count(*) FROM {FILLS_TABLE} WHERE scan_id = ?", [scan_id]
            ).fetchone()[0]
            if int(canonical) <= 0:
                raise RuntimeError("Published Polygon scan has no canonical rows")
            return True
        if status == "published":
            # A newer scan replaced this snapshot.  Its prior leaf coverage has
            # no staged rows after publication, so replay must start from zero
            # rather than falsely short-circuiting or attempting to republish
            # an empty stage.
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(f"DELETE FROM {CHUNKS_TABLE} WHERE scan_id = ?", [scan_id])
                conn.execute(f"DELETE FROM {STAGE_TABLE} WHERE scan_id = ?", [scan_id])
                conn.execute(
                    f"""
                    UPDATE {RUNS_TABLE}
                    SET status = 'running', raw_published = FALSE,
                        finished_at = NULL, published_at = NULL,
                        finalized_head_number = ?, finalized_head_hash = ?,
                        verification_status = 'not_requested',
                        verification_provider_label = NULL,
                        verification_provider_origin = NULL,
                        started_at = ?, error_type = NULL, error_message = NULL
                    WHERE scan_id = ?
                    """,
                    [
                        finalized_head_number,
                        finalized_head_hash,
                        _utc_now(),
                        scan_id,
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            return False
        conn.execute(
            f"""
            UPDATE {RUNS_TABLE}
            SET status = 'running', raw_published = FALSE, finished_at = NULL,
                finalized_head_number = ?, finalized_head_hash = ?,
                error_type = NULL, error_message = NULL
            WHERE scan_id = ?
            """,
            [finalized_head_number, finalized_head_hash, scan_id],
        )
        return False
    conn.execute(
        f"""
        INSERT INTO {RUNS_TABLE} (
            scan_id, manifest_version, manifest_sha256, normalizer_version,
            chain_id, provider_label, provider_origin, finalized_head_number,
            finalized_head_hash, target_ranges_json, boundary_blocks_sha256,
            status, raw_published, verification_status, started_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', FALSE,
                  'not_requested', ?)
        """,
        [scan_id, *expected, _utc_now()],
    )
    return False


def completed_polygon_chunk_ranges(
    conn: duckdb.DuckDBPyConnection, scan_id: str
) -> dict[str, list[tuple[int, int]]]:
    rows = conn.execute(
        f"""
        SELECT exchange_address, from_block, to_block
        FROM {CHUNKS_TABLE}
        WHERE scan_id = ? AND status = 'success'
        ORDER BY exchange_address, from_block, to_block
        """,
        [scan_id],
    ).fetchall()
    result: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for address, from_block, to_block in rows:
        result[str(address)].append((int(from_block), int(to_block)))
    return dict(result)


def record_polygon_settlement_chunk(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    exchange_address: str,
    from_block: int,
    to_block: int,
    from_block_hash: str,
    to_block_hash: str,
    event_count: int,
    scoped_event_count: int,
    scoped_event_sha256: str,
    rows: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, int],
) -> None:
    """Commit one successful leaf and its sanitized normalized rows together."""
    if (
        from_block > to_block
        or event_count < scoped_event_count
        or scoped_event_count < 0
    ):
        raise ValueError("Invalid Polygon chunk counts or range")
    if any(
        row.get("scan_id") != scan_id
        or str(row.get("exchange_address", "")).casefold()
        != exchange_address.casefold()
        or row.get("chunk_from_block") != from_block
        or row.get("chunk_to_block") != to_block
        for row in rows
    ):
        raise ValueError("Staged Polygon rows do not belong to the supplied chunk")
    if set(metrics) != set(CHUNK_METRIC_COLUMNS) or any(
        not isinstance(metrics[column], int)
        or isinstance(metrics[column], bool)
        or metrics[column] < 0
        for column in CHUNK_METRIC_COLUMNS
    ):
        raise ValueError("Invalid Polygon chunk metrics")
    if (
        metrics["eligible_discovery_count"] + metrics["filtered_discovery_count"]
        != metrics["discovery_count"]
        or metrics["receipt_transaction_count"] > metrics["eligible_discovery_count"]
        or scoped_event_count > metrics["receipt_log_count"]
        or event_count != metrics["discovery_count"] + metrics["receipt_log_count"]
    ):
        raise ValueError("Inconsistent Polygon chunk metrics")
    existing = conn.execute(
        f"""
        SELECT from_block_hash, to_block_hash, event_count, scoped_event_count,
               normalized_fill_count, scoped_event_sha256,
               {", ".join(CHUNK_METRIC_COLUMNS)}
        FROM {CHUNKS_TABLE}
        WHERE scan_id = ? AND lower(exchange_address) = lower(?)
          AND from_block = ? AND to_block = ? AND status = 'success'
        """,
        [scan_id, exchange_address, from_block, to_block],
    ).fetchone()
    supplied = (
        from_block_hash,
        to_block_hash,
        event_count,
        scoped_event_count,
        len(rows),
        scoped_event_sha256,
        *(metrics[column] for column in CHUNK_METRIC_COLUMNS),
    )
    if existing:
        if tuple(existing) != supplied:
            raise RuntimeError("Successful Polygon chunk has conflicting payload")
        return
    overlap = conn.execute(
        f"""
        SELECT count(*) FROM {CHUNKS_TABLE}
        WHERE scan_id = ? AND lower(exchange_address) = lower(?)
          AND status = 'success'
          AND NOT (to_block < ? OR from_block > ?)
          AND NOT (from_block = ? AND to_block = ?)
        """,
        [
            scan_id,
            exchange_address,
            from_block,
            to_block,
            from_block,
            to_block,
        ],
    ).fetchone()[0]
    if overlap:
        raise RuntimeError("Successful Polygon scan chunks must not overlap")

    arrow_name = f"polygon_leaf_{uuid.uuid4().hex}"
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            f"""
            DELETE FROM {STAGE_TABLE}
            WHERE scan_id = ? AND lower(exchange_address) = lower(?)
              AND chunk_from_block = ? AND chunk_to_block = ?
            """,
            [scan_id, exchange_address, from_block, to_block],
        )
        if rows:
            conn.register(arrow_name, _fill_arrow_table(rows))
            conn.execute(
                f"INSERT INTO {STAGE_TABLE} ({', '.join(FILL_COLUMNS)}) "
                f"SELECT {', '.join(FILL_COLUMNS)} FROM {arrow_name}"
            )
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {CHUNKS_TABLE} (
                scan_id, exchange_address, from_block, to_block,
                from_block_hash, to_block_hash, status, event_count,
                scoped_event_count, normalized_fill_count, scoped_event_sha256,
                {", ".join(CHUNK_METRIC_COLUMNS)},
                completed_at, error_type, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, 'success', ?, ?, ?, ?,
                      {", ".join("?" for _ in CHUNK_METRIC_COLUMNS)},
                      ?, NULL, NULL)
            """,
            [
                scan_id,
                exchange_address.casefold(),
                from_block,
                to_block,
                from_block_hash,
                to_block_hash,
                event_count,
                scoped_event_count,
                len(rows),
                scoped_event_sha256,
                *(metrics[column] for column in CHUNK_METRIC_COLUMNS),
                _utc_now(),
            ],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        try:
            conn.unregister(arrow_name)
        except duckdb.Error:
            pass


def record_polygon_settlement_failure(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    error: BaseException,
    exchange_address: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
) -> None:
    """Persist only a redacted failure and leave successful leaves resumable."""
    run_state = conn.execute(
        f"SELECT status, raw_published FROM {RUNS_TABLE} WHERE scan_id = ?",
        [scan_id],
    ).fetchone()
    if run_state is None:
        raise RuntimeError(f"Polygon scan {scan_id} does not exist")
    if str(run_state[0]) == "published" and bool(run_state[1]):
        # A concurrent worker may finish this deterministic scan while another
        # attempt is still unwinding.  Late failures must never demote it.
        return
    now = _utc_now()
    message = _clean_error(error)
    if exchange_address is not None and from_block is not None and to_block is not None:
        successful = conn.execute(
            f"""
            SELECT count(*) FROM {CHUNKS_TABLE}
            WHERE scan_id = ? AND lower(exchange_address) = lower(?)
              AND from_block = ? AND to_block = ? AND status = 'success'
            """,
            [scan_id, exchange_address, from_block, to_block],
        ).fetchone()[0]
    else:
        successful = 0
    if (
        not successful
        and exchange_address is not None
        and from_block is not None
        and to_block is not None
    ):
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {CHUNKS_TABLE} (
                scan_id, exchange_address, from_block, to_block, status,
                event_count, scoped_event_count, normalized_fill_count,
                {", ".join(CHUNK_METRIC_COLUMNS)},
                completed_at, error_type, error_message
            )
            SELECT ?, ?, ?, ?, 'failed', 0, 0, 0,
                   {", ".join("0" for _ in CHUNK_METRIC_COLUMNS)}, ?, ?, ?
            WHERE EXISTS (
                SELECT 1 FROM {RUNS_TABLE}
                WHERE scan_id = ?
                  AND NOT (status = 'published' AND raw_published = TRUE)
            )
            """,
            [
                scan_id,
                exchange_address.casefold(),
                from_block,
                to_block,
                now,
                error.__class__.__name__,
                message,
                scan_id,
            ],
        )
    conn.execute(
        f"""
        UPDATE {RUNS_TABLE}
        SET status = 'failed', raw_published = FALSE, finished_at = ?,
            error_type = ?, error_message = ?
        WHERE scan_id = ? AND NOT (status = 'published' AND raw_published = TRUE)
        """,
        [now, error.__class__.__name__, message, scan_id],
    )


def _assert_complete_coverage(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    target_ranges: Sequence[Mapping[str, Any]],
) -> None:
    completed = completed_polygon_chunk_ranges(conn, scan_id)
    expected_addresses = {
        str(item["exchange_address"]).casefold() for item in target_ranges
    }
    if set(completed) != expected_addresses:
        raise RuntimeError("Polygon chunks do not cover every target exchange")
    for address in sorted(expected_addresses):
        leaves = completed[address]
        used = 0
        targets = [
            (int(item["from_block"]), int(item["to_block"]))
            for item in target_ranges
            if str(item["exchange_address"]).casefold() == address
        ]
        for target_start, target_end in targets:
            cursor = target_start
            while used < len(leaves) and leaves[used][0] <= target_end:
                start, end = leaves[used]
                if start != cursor or end > target_end:
                    raise RuntimeError("Polygon chunks contain a gap or overlap")
                cursor = end + 1
                used += 1
            if cursor != target_end + 1:
                raise RuntimeError("Polygon chunks do not fully cover target ranges")
        if used != len(leaves):
            raise RuntimeError("Polygon chunks extend outside target ranges")


def publish_polygon_settlement_scan(
    conn: duckdb.DuckDBPyConnection,
    *,
    scan_id: str,
    target_ranges: Sequence[Mapping[str, Any]],
) -> int:
    """Validate all leaves and atomically replace the canonical fill snapshot."""
    ranges_json = _validated_target_ranges_json(target_ranges)
    run_state = conn.execute(
        f"SELECT status, raw_published, target_ranges_json "
        f"FROM {RUNS_TABLE} WHERE scan_id = ?",
        [scan_id],
    ).fetchone()
    if run_state is None:
        raise RuntimeError(f"Polygon scan {scan_id} does not exist")
    try:
        stored_ranges_json = _validated_target_ranges_json(
            json.loads(str(run_state[2]))
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Polygon scan target-range provenance is invalid") from exc
    if stored_ranges_json != ranges_json:
        raise RuntimeError("Polygon scan target-range provenance is incompatible")
    if str(run_state[0]) == "published" and bool(run_state[1]):
        canonical = int(
            conn.execute(
                f"SELECT count(*) FROM {FILLS_TABLE} WHERE scan_id = ?", [scan_id]
            ).fetchone()[0]
        )
        if canonical <= 0:
            raise RuntimeError("Published Polygon scan has no canonical rows")
        return canonical
    _assert_complete_coverage(
        conn,
        scan_id=scan_id,
        target_ranges=target_ranges,
    )
    staged = int(
        conn.execute(
            f"SELECT count(*) FROM {STAGE_TABLE} WHERE scan_id = ?", [scan_id]
        ).fetchone()[0]
    )
    expected = int(
        conn.execute(
            f"""
            SELECT coalesce(sum(normalized_fill_count), 0)
            FROM {CHUNKS_TABLE}
            WHERE scan_id = ? AND status = 'success'
            """,
            [scan_id],
        ).fetchone()[0]
    )
    if staged <= 0 or staged != expected:
        raise RuntimeError(
            f"Polygon stage must be non-empty and match successful chunks: {staged}/{expected}"
        )

    columns = ", ".join(FILL_COLUMNS)
    now = _utc_now()
    conn.execute("BEGIN TRANSACTION")
    try:
        # scan_chunks is the resumable current-coverage state, not an
        # append-only attempt log.  Parent-range failures can overlap adaptive
        # leaves which were committed before a later sibling failed.  Once
        # coverage is proven complete those failed attempts are superseded and
        # must not leak into the published dbt audit contract.
        conn.execute(
            f"DELETE FROM {CHUNKS_TABLE} WHERE scan_id = ? AND status = 'failed'",
            [scan_id],
        )
        conn.execute(
            f"UPDATE {RUNS_TABLE} SET raw_published = FALSE "
            "WHERE scan_id <> ? AND raw_published = TRUE",
            [scan_id],
        )
        conn.execute(f"DELETE FROM {FILLS_TABLE}")
        conn.execute(
            f"INSERT INTO {FILLS_TABLE} ({columns}) "
            f"SELECT {columns} FROM {STAGE_TABLE} WHERE scan_id = ?",
            [scan_id],
        )
        updated = conn.execute(
            f"""
            UPDATE {RUNS_TABLE}
            SET status = 'published', raw_published = TRUE, finished_at = ?,
                published_at = ?, error_type = NULL, error_message = NULL
            WHERE scan_id = ? AND status IN ('running', 'failed')
            """,
            [now, now, scan_id],
        ).fetchone()[0]
        if int(updated) != 1:
            raise RuntimeError(f"Polygon scan {scan_id} is not publishable")
        conn.execute(f"DELETE FROM {STAGE_TABLE} WHERE scan_id = ?", [scan_id])
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return staged


def set_polygon_verification_status(
    conn: duckdb.DuckDBPyConnection,
    scan_id: str,
    status: str,
    *,
    provider_label: str | None = None,
    provider_origin: str | None = None,
) -> None:
    if status not in {"not_requested", "matched", "mismatched", "error"}:
        raise ValueError("Invalid Polygon verification status")
    if provider_label is not None:
        provider_label = validate_polygon_provider_label(
            provider_label,
            field="verification provider_label",
        )
    if provider_origin is not None:
        provider_origin = _validated_provider_origin(provider_origin)
    conn.execute(
        f"""
        UPDATE {RUNS_TABLE}
        SET verification_status = ?, verification_provider_label = ?,
            verification_provider_origin = ?
        WHERE scan_id = ?
        """,
        [status, provider_label, provider_origin, scan_id],
    )


def load_polygon_settlement_release_provenance(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """Return release-safe provenance for the one current canonical snapshot."""
    scan_ids = conn.execute(f"SELECT DISTINCT scan_id FROM {FILLS_TABLE}").fetchall()
    if len(scan_ids) != 1:
        raise RuntimeError("Expected exactly one canonical Polygon settlement scan")
    scan_id = str(scan_ids[0][0])
    row = conn.execute(
        f"""
        SELECT manifest_sha256, manifest_version, chain_id, finalized_head_number,
               finalized_head_hash, normalizer_version, published_at,
               provider_label, provider_origin, verification_status,
               verification_provider_label, verification_provider_origin
        FROM {RUNS_TABLE}
        WHERE scan_id = ? AND status = 'published' AND raw_published = TRUE
        """,
        [scan_id],
    ).fetchone()
    if row is None:
        raise RuntimeError("Canonical Polygon settlement scan is not published")
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
    return {
        "scan_id": scan_id,
        "seed_sha256": row[0],
        "seed_version": row[1],
        "chain_id": int(row[2]),
        "exchange_addresses": sorted({str(chunk[0]) for chunk in chunks}),
        "finalized_head_block_number": int(row[3]),
        "finalized_head_block_hash": str(row[4]),
        "block_ranges": [
            {
                "exchange_address": chunk[0],
                "from_block": int(chunk[1]),
                "to_block": int(chunk[2]),
                "from_block_hash": chunk[3],
                "to_block_hash": chunk[4],
                "chunk_sha256": chunk[5],
            }
            for chunk in chunks
        ],
        "normalizer_version": row[5],
        "scan_published_at_utc": row[6],
        "rpc_provider_label": row[7],
        "rpc_provider_origin": row[8],
        "verification_status": row[9],
        "verification_rpc_provider_label": row[10],
        "verification_rpc_provider_origin": row[11],
    }


__all__ = [
    "CHUNKS_TABLE",
    "FILLS_TABLE",
    "FILL_COLUMNS",
    "RUNS_TABLE",
    "STAGE_TABLE",
    "completed_polygon_chunk_ranges",
    "load_polygon_settlement_release_provenance",
    "publish_polygon_settlement_scan",
    "record_polygon_settlement_chunk",
    "record_polygon_settlement_failure",
    "set_polygon_verification_status",
    "start_polygon_settlement_scan",
    "validate_polygon_provider_label",
]
