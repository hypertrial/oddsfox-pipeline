#!/usr/bin/env python3
"""Validate exact v3/v4 Polygon outputs and write a sanitized benchmark report."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

RAW_TABLE = "polymarket_wc2026_raw.polygon_settlement_fills"
RUNS_TABLE = "polymarket_wc2026_ops.polygon_settlement_scan_runs"
CHUNKS_TABLE = "polymarket_wc2026_ops.polygon_settlement_scan_chunks"
MART_TABLE = "polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds"
QUALITY_TABLE = (
    "polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_data_quality"
)
EXPECTED_MART_ROWS = 39_120
ECONOMIC_EXCLUSIONS = frozenset(
    {
        "scan_id",
        "chunk_from_block",
        "chunk_to_block",
        "decoder_version",
        "ingested_at",
    }
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _columns(conn: duckdb.DuckDBPyConnection, catalog: str, relation: str) -> list[str]:
    schema, table = relation.split(".", 1)
    return [
        str(row[0])
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [catalog, schema, table],
        ).fetchall()
    ]


def _difference_count(
    conn: duckdb.DuckDBPyConnection,
    relation: str,
    columns: list[str],
) -> int:
    if not columns:
        raise RuntimeError(f"Benchmark relation {relation} has no comparable columns")
    projection = ", ".join(f'"{column}"' for column in columns)
    return int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM (
                (SELECT {projection} FROM v3.{relation}
                 EXCEPT ALL
                 SELECT {projection} FROM v4.{relation})
                UNION ALL
                (SELECT {projection} FROM v4.{relation}
                 EXCEPT ALL
                 SELECT {projection} FROM v3.{relation})
            ) AS differences
            """
        ).fetchone()[0]
    )


def _run_summary(conn: duckdb.DuckDBPyConnection, catalog: str) -> dict[str, object]:
    runs = conn.execute(
        f"""
        SELECT scan_id, normalizer_version, manifest_sha256, started_at, published_at
        FROM {catalog}.{RUNS_TABLE}
        WHERE status = 'published' AND raw_published = TRUE
        """
    ).fetchall()
    if len(runs) != 1:
        raise RuntimeError(f"{catalog} must contain exactly one published scan")
    scan_id, normalizer, manifest_sha256, started_at, published_at = runs[0]
    if started_at is None or published_at is None or published_at < started_at:
        raise RuntimeError(f"{catalog} published scan has invalid timestamps")
    chunk = conn.execute(
        f"""
        SELECT count(*), coalesce(sum(to_block - from_block + 1), 0),
               coalesce(sum(event_count), 0),
               coalesce(sum(normalized_fill_count), 0),
               count(*) FILTER (WHERE status <> 'success')
        FROM {catalog}.{CHUNKS_TABLE}
        WHERE scan_id = ?
        """,
        [scan_id],
    ).fetchone()
    fill_count = int(
        conn.execute(
            f"SELECT count(*) FROM {catalog}.{RAW_TABLE} WHERE scan_id = ?",
            [scan_id],
        ).fetchone()[0]
    )
    if int(chunk[4]) != 0 or fill_count <= 0 or fill_count != int(chunk[3]):
        raise RuntimeError(f"{catalog} scan is incomplete or internally inconsistent")
    return {
        "normalizer_version": str(normalizer),
        "manifest_sha256": str(manifest_sha256),
        "duration_seconds": round((published_at - started_at).total_seconds(), 6),
        "block_count": int(chunk[1]),
        "chunk_count": int(chunk[0]),
        "event_count": int(chunk[2]),
        "fill_count": fill_count,
    }


def compare_polygon_benchmarks(
    v3_path: Path, v4_path: Path, output_path: Path
) -> dict[str, object]:
    v3_path = v3_path.resolve()
    v4_path = v4_path.resolve()
    output_path = output_path.resolve()
    if v3_path == v4_path:
        raise ValueError("v3 and v4 benchmark databases must be different files")
    if not v3_path.is_file() or not v4_path.is_file():
        raise FileNotFoundError("Both completed benchmark databases are required")

    conn = duckdb.connect(":memory:")
    try:
        conn.execute(f"ATTACH {_sql_literal(str(v3_path))} AS v3 (READ_ONLY)")
        conn.execute(f"ATTACH {_sql_literal(str(v4_path))} AS v4 (READ_ONLY)")
        raw_v3 = _columns(conn, "v3", RAW_TABLE)
        raw_v4 = _columns(conn, "v4", RAW_TABLE)
        economic_v3 = [column for column in raw_v3 if column not in ECONOMIC_EXCLUSIONS]
        economic_v4 = [column for column in raw_v4 if column not in ECONOMIC_EXCLUSIONS]
        if economic_v3 != economic_v4:
            raise RuntimeError("v3/v4 economic fill schemas differ")
        fill_differences = _difference_count(conn, RAW_TABLE, economic_v3)
        if fill_differences:
            raise RuntimeError(
                f"v3/v4 economic fills differ in {fill_differences} rows"
            )

        mart_v3 = _columns(conn, "v3", MART_TABLE)
        mart_v4 = _columns(conn, "v4", MART_TABLE)
        if mart_v3 != mart_v4:
            raise RuntimeError("v3/v4 mart schemas differ")
        mart_differences = _difference_count(conn, MART_TABLE, mart_v3)
        if mart_differences:
            raise RuntimeError(f"v3/v4 marts differ in {mart_differences} rows")
        mart_counts = {
            catalog: int(
                conn.execute(f"SELECT count(*) FROM {catalog}.{MART_TABLE}").fetchone()[
                    0
                ]
            )
            for catalog in ("v3", "v4")
        }
        if set(mart_counts.values()) != {EXPECTED_MART_ROWS}:
            raise RuntimeError("Both benchmark marts must contain exactly 39,120 rows")

        quality = conn.execute(
            f"SELECT publication_ready, blocking_issue_keys FROM v4.{QUALITY_TABLE}"
        ).fetchall()
        if quality != [(True, None)]:
            raise RuntimeError("v4 publication gates did not pass")
        v3 = _run_summary(conn, "v3")
        v4 = _run_summary(conn, "v4")
        metric_columns = set(_columns(conn, "v4", CHUNKS_TABLE))
        required_metrics = {
            "http_request_count",
            "log_rpc_call_count",
            "receipt_rpc_call_count",
            "header_rpc_call_count",
            "retry_count",
            "adaptive_split_count",
        }
        if not required_metrics <= metric_columns:
            raise RuntimeError("v4 benchmark is missing RPC metrics")
        rpc_values = conn.execute(
            f"""
            SELECT coalesce(sum(http_request_count), 0),
                   coalesce(sum(log_rpc_call_count), 0),
                   coalesce(sum(receipt_rpc_call_count), 0),
                   coalesce(sum(header_rpc_call_count), 0),
                   coalesce(sum(retry_count), 0),
                   coalesce(sum(adaptive_split_count), 0)
            FROM v4.{CHUNKS_TABLE}
            WHERE status = 'success'
            """
        ).fetchone()
    finally:
        conn.close()

    v3_duration = float(v3["duration_seconds"])
    v4_duration = float(v4["duration_seconds"])
    report: dict[str, object] = {
        "benchmark_version": 1,
        "generated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "equality": {
            "economic_fill_differences": 0,
            "mart_differences": 0,
            "mart_rows": EXPECTED_MART_ROWS,
        },
        "v3": {**v3, "database_sha256": _sha256(v3_path)},
        "v4": {
            **v4,
            "database_sha256": _sha256(v4_path),
            "rpc": {
                "http_requests": int(rpc_values[0]),
                "log_calls": int(rpc_values[1]),
                "receipt_calls": int(rpc_values[2]),
                "header_calls": int(rpc_values[3]),
                "retries": int(rpc_values[4]),
                "adaptive_splits": int(rpc_values[5]),
            },
        },
        "speed_ratio_v3_over_v4": (
            round(v3_duration / v4_duration, 6) if v4_duration > 0 else None
        ),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output_path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v3-duckdb", required=True, type=Path)
    parser.add_argument("--v4-duckdb", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        report = compare_polygon_benchmarks(args.v3_duckdb, args.v4_duckdb, args.output)
    except (duckdb.Error, FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=os.sys.stderr)
        return 1
    print(
        "Polygon benchmark matched exactly; v3/v4 speed ratio: "
        f"{report['speed_ratio_v3_over_v4']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
