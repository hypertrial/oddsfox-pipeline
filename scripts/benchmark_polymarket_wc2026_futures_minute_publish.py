#!/usr/bin/env python3
"""Benchmark futures-minute publish: heap baseline vs Parquet snapshot.

Measures only spill/stage + DuckDB publish (not CLOB fetch or dbt). Uses
synthetic token histories and disposable DuckDB files under
``${ODDSFOX_RUNTIME_ROOT}/benchmarks/futures-minute-publish/``. Never opens the
operator warehouse.

Smoke/tune build Python fetch results then exercise the production spill API.
Performance/production-shaped stream synthetic Parquet with DuckDB so the
publish ratio is measurable without holding hundreds of millions of Python
tuples in RAM.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from oddsfox_pipeline.ingestion.polymarket.odds.minute_batch import (
    MinuteFetchResult,
    build_minute_history_arrow_table,
    cleanup_minute_odds_publish_cache,
    minute_odds_publish_cache_dir,
    write_minute_history_parquet_shards,
)
from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    baseline_publish_minute_odds_from_table,
    load_futures_minute_fetch_audit,
    load_futures_minute_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_q,
    polymarket_raw_schema,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_all_polymarket_tables,
)

TIER_SIZES = {
    "smoke": (8, 64),
    "tune": (40, 50_000),
    "performance": (200, 50_000),
    "production-shaped": (60_000, 6_284),
}

MATRIX = (
    (1_000_000, "uncompressed"),
    (1_000_000, "snappy"),
    (1_000_000, "zstd"),
    (2_000_000, "uncompressed"),
    (2_000_000, "snappy"),
    (2_000_000, "zstd"),
    (4_000_000, "uncompressed"),
    (4_000_000, "snappy"),
    (4_000_000, "zstd"),
)

_STREAM_TIERS = frozenset({"performance", "production-shaped"})


@dataclass(frozen=True)
class _Plan:
    market_id: str
    token_id: str
    started_at: datetime
    finished_at: datetime


def _peak_rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if os.uname().sysname == "Darwin":
        return int(usage)
    return int(usage) * 1024


def _parquet_compression(label: str) -> str | None:
    if label in {"none", "uncompressed"}:
        return None
    return label


def _synthetic_results(
    *,
    tokens: int,
    rows_per_token: int,
) -> list[MinuteFetchResult]:
    start = datetime(2026, 6, 11, tzinfo=timezone.utc)
    end = datetime(2026, 7, 19, tzinfo=timezone.utc)
    base_ts = int(start.timestamp())
    now = datetime.now(timezone.utc)
    results: list[MinuteFetchResult] = []
    for index in range(tokens):
        token_id = f"token-{index:05d}"
        market_id = f"market-{index:05d}"
        history = tuple(
            (token_id, base_ts + offset * 60, 0.1 + ((offset + index) % 80) / 100.0)
            for offset in range(rows_per_token)
        )
        results.append(
            MinuteFetchResult(
                plan=_Plan(
                    market_id=market_id,
                    token_id=token_id,
                    started_at=start,
                    finished_at=end,
                ),
                fetch_status="success",
                history=history,
                request_start_epoch=base_ts,
                request_end_epoch=base_ts + rows_per_token * 60,
                source_row_count=rows_per_token,
                history_sha256="a" * 64,
                fetch_started_at=now,
                fetch_finished_at=now,
            )
        )
    return results


def _audit_rows_from_results(
    results: list[MinuteFetchResult], fetch_run_id: str
) -> list[dict]:
    return [
        {
            "fetch_run_id": fetch_run_id,
            "market_id": result.plan.market_id,
            "clobTokenId": result.plan.token_id,
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": result.plan.started_at,
            "exact_window_end_at": result.plan.finished_at,
            "request_start_epoch": result.request_start_epoch,
            "request_end_epoch": result.request_end_epoch,
            "source_row_count": result.source_row_count,
            "window_row_count": len(result.history),
            "window_history_sha256": result.history_sha256,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": result.fetch_started_at,
            "fetch_finished_at": result.fetch_finished_at,
            "error_type": None,
            "error_message": None,
        }
        for result in results
    ]


def _audit_rows_synthetic(
    *,
    tokens: int,
    rows_per_token: int,
    fetch_run_id: str,
) -> list[dict]:
    start = datetime(2026, 6, 11, tzinfo=timezone.utc)
    end = datetime(2026, 7, 19, tzinfo=timezone.utc)
    base_ts = int(start.timestamp())
    now = datetime(2026, 8, 1, tzinfo=timezone.utc)
    return [
        {
            "fetch_run_id": fetch_run_id,
            "market_id": f"market-{index:05d}",
            "clobTokenId": f"token-{index:05d}",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": start,
            "exact_window_end_at": end,
            "request_start_epoch": base_ts,
            "request_end_epoch": base_ts + rows_per_token * 60,
            "source_row_count": rows_per_token,
            "window_row_count": rows_per_token,
            "window_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }
        for index in range(tokens)
    ]


def _bootstrap(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS polymarket_wc2026_raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS polymarket_wc2026_ops")
    bootstrap_all_polymarket_tables(conn)
    return conn


def _attach_readonly(conn: duckdb.DuckDBPyConnection, path: Path, alias: str) -> None:
    conn.execute(f"ATTACH '{path.as_posix()}' AS {alias} (READ_ONLY)")


def _tables_equal(
    left_db: Path,
    right_db: Path,
    *,
    fetch_run_id_left: str,
    fetch_run_id_right: str,
) -> dict[str, bool]:
    """Exact equality via EXCEPT ALL (no Python materialization of raw rows)."""
    conn = duckdb.connect(":memory:")
    try:
        _attach_readonly(conn, left_db, "baseline")
        _attach_readonly(conn, right_db, "candidate")
        raw_diff = int(
            conn.execute(
                """
                SELECT count(*) FROM (
                    SELECT * FROM baseline.polymarket_wc2026_raw.futures_minute_odds_history
                    EXCEPT ALL
                    SELECT * FROM candidate.polymarket_wc2026_raw.futures_minute_odds_history
                )
                """
            ).fetchone()[0]
        )
        raw_diff_rev = int(
            conn.execute(
                """
                SELECT count(*) FROM (
                    SELECT * FROM candidate.polymarket_wc2026_raw.futures_minute_odds_history
                    EXCEPT ALL
                    SELECT * FROM baseline.polymarket_wc2026_raw.futures_minute_odds_history
                )
                """
            ).fetchone()[0]
        )
        audit_diff = int(
            conn.execute(
                """
                SELECT count(*) FROM (
                    SELECT clobTokenId, fetch_status, raw_published, window_row_count
                    FROM baseline.polymarket_wc2026_ops.futures_minute_odds_fetch_audit
                    WHERE fetch_run_id = ?
                    EXCEPT ALL
                    SELECT clobTokenId, fetch_status, raw_published, window_row_count
                    FROM candidate.polymarket_wc2026_ops.futures_minute_odds_fetch_audit
                    WHERE fetch_run_id = ?
                )
                """,
                [fetch_run_id_left, fetch_run_id_right],
            ).fetchone()[0]
        )
        audit_diff_rev = int(
            conn.execute(
                """
                SELECT count(*) FROM (
                    SELECT clobTokenId, fetch_status, raw_published, window_row_count
                    FROM candidate.polymarket_wc2026_ops.futures_minute_odds_fetch_audit
                    WHERE fetch_run_id = ?
                    EXCEPT ALL
                    SELECT clobTokenId, fetch_status, raw_published, window_row_count
                    FROM baseline.polymarket_wc2026_ops.futures_minute_odds_fetch_audit
                    WHERE fetch_run_id = ?
                )
                """,
                [fetch_run_id_right, fetch_run_id_left],
            ).fetchone()[0]
        )
        return {
            "raw_identical": raw_diff == 0 and raw_diff_rev == 0,
            "audit_identical": audit_diff == 0 and audit_diff_rev == 0,
        }
    finally:
        conn.close()


def _write_streamed_shards(
    *,
    tokens: int,
    rows_per_token: int,
    fetch_run_id: str,
    shard_rows: int,
    compression_label: str,
) -> tuple[list[Path], float, int]:
    """Generate synthetic Parquet shards with DuckDB (no giant Python histories)."""
    cache_dir = minute_odds_publish_cache_dir(fetch_run_id)
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 6, 11, tzinfo=timezone.utc)
    end = datetime(2026, 7, 19, tzinfo=timezone.utc)
    ingested = datetime(2026, 8, 1, tzinfo=timezone.utc)
    base_ts = int(start.timestamp())
    total_rows = tokens * rows_per_token
    codec = _parquet_compression(compression_label)
    compression_sql = {
        None: "UNCOMPRESSED",
        "snappy": "SNAPPY",
        "zstd": "ZSTD",
    }[codec]
    shard_cap = max(1, int(shard_rows))
    # Whole tokens per shard when possible (rows_per_token may exceed shard_cap).
    tokens_per_shard = max(1, shard_cap // max(1, rows_per_token))
    started = time.perf_counter()
    conn = duckdb.connect(":memory:")
    shard_paths: list[Path] = []
    try:
        shard_index = 0
        token_offset = 0
        while token_offset < tokens:
            token_count = min(tokens_per_shard, tokens - token_offset)
            path = cache_dir / f"shard-{shard_index:05d}.parquet"
            conn.execute(
                f"""
                COPY (
                    SELECT
                        printf('market-%05d', {token_offset} + token_idx) AS market_id,
                        printf('token-%05d', {token_offset} + token_idx) AS "clobTokenId",
                        (?::BIGINT + row_idx * 60) AS timestamp,
                        (
                            0.1
                            + ((row_idx + {token_offset} + token_idx) % 80) / 100.0
                        )::DOUBLE AS price,
                        1::INTEGER AS fidelity_minutes,
                        ?::TIMESTAMPTZ AS window_start_at,
                        ?::TIMESTAMPTZ AS window_end_at,
                        ?::TIMESTAMPTZ AS ingested_at
                    FROM range({token_count}) AS t(token_idx)
                    CROSS JOIN range({rows_per_token}) AS r(row_idx)
                ) TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION {compression_sql})
                """,
                [base_ts, start, end, ingested],
            )
            shard_paths.append(path)
            token_offset += token_count
            shard_index += 1
    finally:
        conn.close()
    (cache_dir / "manifest.json").write_text(
        json.dumps(
            {
                "fetch_run_id": fetch_run_id,
                "token_count": tokens,
                "row_count": total_rows,
                "shard_count": len(shard_paths),
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return shard_paths, time.perf_counter() - started, total_rows


def _baseline_publish_from_parquet(
    conn: duckdb.DuckDBPyConnection,
    parquet_paths: list[Path],
    *,
    fetch_run_id: str,
) -> tuple[int, float]:
    """Legacy stage/DELETE/window INSERT oracle fed from Parquet (benchmark only)."""
    schema = polymarket_raw_schema(SCOPE_WC2026)
    stage_name = "stage_futures_minute_odds_history_v1"
    stage = polymarket_q(schema, stage_name)
    target = polymarket_raw_tbl(SCOPE_WC2026, "futures_minute_odds_history")
    audit = polymarket_ops_tbl(SCOPE_WC2026, "futures_minute_odds_fetch_audit")
    path_literals = ", ".join("?" for _ in parquet_paths)
    started = time.perf_counter()
    conn.execute(f"DROP TABLE IF EXISTS {stage}")
    conn.execute(
        f"""
        CREATE TABLE {stage} AS
        SELECT
            market_id,
            "clobTokenId" AS clob_token_id,
            timestamp,
            price,
            fidelity_minutes,
            window_start_at,
            window_end_at,
            ingested_at,
            (row_number() OVER () - 1)::BIGINT AS row_order
        FROM read_parquet([{path_literals}])
        """,
        [str(path) for path in parquet_paths],
    )
    conn.execute("BEGIN TRANSACTION")
    try:
        stage_tokens = int(
            conn.execute(
                f"SELECT count(DISTINCT clob_token_id) FROM {stage}"
            ).fetchone()[0]
        )
        success_unpublished = int(
            conn.execute(
                f"""
                SELECT count(*) FILTER (
                    WHERE fetch_status = 'success' AND NOT raw_published
                )
                FROM {audit}
                WHERE fetch_run_id = ?
                """,
                [fetch_run_id],
            ).fetchone()[0]
        )
        if success_unpublished != stage_tokens:
            raise RuntimeError(
                f"Fetch audit inventory does not match {stage_tokens} staged tokens "
                f"for run {fetch_run_id}: success_unpublished={success_unpublished}"
            )
        conn.execute(f"DELETE FROM {target}")
        conn.execute(
            f"""
            INSERT INTO {target}
            (market_id, clobTokenId, timestamp, price, fidelity_minutes,
             window_start_at, window_end_at, ingested_at)
            SELECT market_id, clob_token_id, timestamp, price, fidelity_minutes,
                   window_start_at, window_end_at, ingested_at
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY clob_token_id, timestamp
                    ORDER BY ingested_at DESC, row_order DESC
                ) AS rn
                FROM {stage}
            )
            WHERE rn = 1
            """
        )
        updated = conn.execute(
            f"""
            UPDATE {audit}
            SET raw_published = TRUE
            WHERE fetch_run_id = ?
              AND fetch_status = 'success'
            """,
            [fetch_run_id],
        ).fetchone()[0]
        if int(updated) != stage_tokens:
            raise RuntimeError(
                f"Published {updated} audit rows for {stage_tokens} staged tokens "
                f"in run {fetch_run_id}"
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return stage_tokens, time.perf_counter() - started


def _run_baseline_results(
    results: list[MinuteFetchResult],
    *,
    duckdb_path: Path,
    fetch_run_id: str,
) -> dict[str, Any]:
    conn = _bootstrap(duckdb_path)
    try:
        load_futures_minute_fetch_audit(
            _audit_rows_from_results(results, fetch_run_id), conn
        )
        t0 = time.perf_counter()
        table = build_minute_history_arrow_table(
            results, ingested_at=datetime(2026, 8, 1, tzinfo=timezone.utc)
        )
        arrow_s = time.perf_counter() - t0
        t1 = time.perf_counter()
        tokens = baseline_publish_minute_odds_from_table(
            conn,
            table,
            relation="futures_minute_odds_history",
            fetch_run_id=fetch_run_id,
            audit_mode="success_only",
        )
        publish_s = time.perf_counter() - t1
        return {
            "tokens": int(tokens),
            "rows": int(table.num_rows),
            "arrow_build_seconds": arrow_s,
            "publish_seconds": publish_s,
            "total_seconds": arrow_s + publish_s,
            "peak_rss_bytes": _peak_rss_bytes(),
            "duckdb_bytes": duckdb_path.stat().st_size if duckdb_path.exists() else 0,
            "fetch_run_id": fetch_run_id,
        }
    finally:
        conn.close()


def _run_candidate_results(
    results: list[MinuteFetchResult],
    *,
    duckdb_path: Path,
    fetch_run_id: str,
    shard_rows: int,
    compression_label: str,
) -> dict[str, Any]:
    conn = _bootstrap(duckdb_path)
    try:
        load_futures_minute_fetch_audit(
            _audit_rows_from_results(results, fetch_run_id), conn
        )
        t0 = time.perf_counter()
        shards = write_minute_history_parquet_shards(
            results,
            fetch_run_id=fetch_run_id,
            ingested_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            max_rows_per_shard=shard_rows,
            compression=_parquet_compression(compression_label),
        )
        spill_s = time.perf_counter() - t0
        shard_bytes = sum(path.stat().st_size for path in shards)
        rows = sum(int(pq.ParquetFile(path).metadata.num_rows) for path in shards)
        t1 = time.perf_counter()
        load_futures_minute_odds_history_stage(shards, conn, fetch_run_id=fetch_run_id)
        publish_s = time.perf_counter() - t1
        tokens = int(
            conn.execute(
                "SELECT count(DISTINCT clobTokenId) "
                "FROM polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
        )
        return {
            "tokens": tokens,
            "rows": int(rows),
            "shard_count": len(shards),
            "shard_bytes": shard_bytes,
            "spill_seconds": spill_s,
            "publish_seconds": publish_s,
            "total_seconds": spill_s + publish_s,
            "peak_rss_bytes": _peak_rss_bytes(),
            "duckdb_bytes": duckdb_path.stat().st_size if duckdb_path.exists() else 0,
            "fetch_run_id": fetch_run_id,
        }
    finally:
        conn.close()
        cleanup_minute_odds_publish_cache(fetch_run_id)


def _run_streamed_pair(
    *,
    tokens: int,
    rows_per_token: int,
    work: Path,
    shard_rows: int,
    compression_label: str,
) -> dict[str, Any]:
    """Shared streamed Parquet input; compare DuckDB baseline vs candidate publish."""
    shared_run = f"bench-shared-{shard_rows}-{compression_label}"
    shards, gen_s, total_rows = _write_streamed_shards(
        tokens=tokens,
        rows_per_token=rows_per_token,
        fetch_run_id=shared_run,
        shard_rows=shard_rows,
        compression_label=compression_label,
    )
    shard_bytes = sum(path.stat().st_size for path in shards)
    baseline_path = work / f"baseline-{shard_rows}-{compression_label}.duckdb"
    candidate_path = work / f"candidate-{shard_rows}-{compression_label}.duckdb"
    baseline_run = f"bench-base-{shard_rows}-{compression_label}"
    candidate_run = f"bench-cand-{shard_rows}-{compression_label}"

    conn = _bootstrap(baseline_path)
    try:
        load_futures_minute_fetch_audit(
            _audit_rows_synthetic(
                tokens=tokens,
                rows_per_token=rows_per_token,
                fetch_run_id=baseline_run,
            ),
            conn,
        )
        base_tokens, base_publish_s = _baseline_publish_from_parquet(
            conn, shards, fetch_run_id=baseline_run
        )
    finally:
        conn.close()
    baseline = {
        "tokens": base_tokens,
        "rows": total_rows,
        "arrow_build_seconds": 0.0,
        "publish_seconds": base_publish_s,
        "total_seconds": base_publish_s,
        "peak_rss_bytes": _peak_rss_bytes(),
        "duckdb_bytes": baseline_path.stat().st_size if baseline_path.exists() else 0,
        "fetch_run_id": baseline_run,
        "input": "shared_streamed_parquet",
    }

    # Re-copy shards into candidate fetch_run_id cache with manifest.
    cand_dir = minute_odds_publish_cache_dir(candidate_run)
    if cand_dir.exists():
        shutil.rmtree(cand_dir)
    cand_dir.mkdir(parents=True, exist_ok=True)
    cand_shards = []
    for path in shards:
        dest = cand_dir / path.name
        shutil.copy2(path, dest)
        cand_shards.append(dest)
    shutil.copy2(
        minute_odds_publish_cache_dir(shared_run) / "manifest.json",
        cand_dir / "manifest.json",
    )
    # Rewrite manifest fetch_run_id.
    manifest = json.loads((cand_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["fetch_run_id"] = candidate_run
    (cand_dir / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8"
    )

    conn = _bootstrap(candidate_path)
    try:
        load_futures_minute_fetch_audit(
            _audit_rows_synthetic(
                tokens=tokens,
                rows_per_token=rows_per_token,
                fetch_run_id=candidate_run,
            ),
            conn,
        )
        t1 = time.perf_counter()
        load_futures_minute_odds_history_stage(
            cand_shards, conn, fetch_run_id=candidate_run
        )
        cand_publish_s = time.perf_counter() - t1
        cand_tokens = int(
            conn.execute(
                "SELECT count(DISTINCT clobTokenId) "
                "FROM polymarket_wc2026_raw.futures_minute_odds_history"
            ).fetchone()[0]
        )
    finally:
        conn.close()
        cleanup_minute_odds_publish_cache(candidate_run)
        cleanup_minute_odds_publish_cache(shared_run)

    candidate = {
        "tokens": cand_tokens,
        "rows": total_rows,
        "shard_count": len(shards),
        "shard_bytes": shard_bytes,
        "spill_seconds": gen_s,
        "publish_seconds": cand_publish_s,
        # Publish-only ratio denominator: shared generation is setup, not the
        # snapshot publish contract under test for streamed tiers.
        "total_seconds": cand_publish_s,
        "peak_rss_bytes": _peak_rss_bytes(),
        "duckdb_bytes": candidate_path.stat().st_size if candidate_path.exists() else 0,
        "fetch_run_id": candidate_run,
        "input": "shared_streamed_parquet",
        "generation_seconds": gen_s,
    }
    equality = _tables_equal(
        baseline_path,
        candidate_path,
        fetch_run_id_left=baseline_run,
        fetch_run_id_right=candidate_run,
    )
    speed_ratio = (
        float(baseline["total_seconds"]) / float(candidate["total_seconds"])
        if float(candidate["total_seconds"]) > 0
        else 0.0
    )
    return {
        "shard_rows": shard_rows,
        "compression": compression_label,
        "equality": equality,
        "baseline": baseline,
        "candidate": candidate,
        "speed_ratio_baseline_over_candidate": speed_ratio,
    }


def _compare_one_results(
    *,
    results: list[MinuteFetchResult],
    work: Path,
    shard_rows: int,
    compression_label: str,
) -> dict[str, Any]:
    baseline_path = work / f"baseline-{shard_rows}-{compression_label}.duckdb"
    candidate_path = work / f"candidate-{shard_rows}-{compression_label}.duckdb"
    baseline_run = f"bench-base-{shard_rows}-{compression_label}"
    candidate_run = f"bench-cand-{shard_rows}-{compression_label}"
    baseline = _run_baseline_results(
        results,
        duckdb_path=baseline_path,
        fetch_run_id=baseline_run,
    )
    candidate = _run_candidate_results(
        results,
        duckdb_path=candidate_path,
        fetch_run_id=candidate_run,
        shard_rows=shard_rows,
        compression_label=compression_label,
    )
    equality = _tables_equal(
        baseline_path,
        candidate_path,
        fetch_run_id_left=baseline_run,
        fetch_run_id_right=candidate_run,
    )
    speed_ratio = (
        float(baseline["total_seconds"]) / float(candidate["total_seconds"])
        if float(candidate["total_seconds"]) > 0
        else 0.0
    )
    return {
        "shard_rows": shard_rows,
        "compression": compression_label,
        "equality": equality,
        "baseline": baseline,
        "candidate": candidate,
        "speed_ratio_baseline_over_candidate": speed_ratio,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tier",
        choices=("smoke", "tune", "performance", "production-shaped"),
        default="smoke",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-rows", type=int, default=4_000_000)
    parser.add_argument(
        "--compression",
        choices=("snappy", "zstd", "uncompressed", "none"),
        default="snappy",
    )
    parser.add_argument(
        "--matrix",
        action="store_true",
        help="Run the fixed 1M/2M/4M × uncompressed/snappy/zstd matrix.",
    )
    parser.add_argument("--require-speedup", type=float, default=0.0)
    args = parser.parse_args(argv)

    tokens, rows_per_token = TIER_SIZES[args.tier]
    root = Path(
        os.getenv("ODDSFOX_RUNTIME_ROOT", str(Path.cwd() / ".cache" / "runtime"))
    ).resolve()
    bench_root = root / "benchmarks" / "futures-minute-publish"
    bench_root.mkdir(parents=True, exist_ok=True)
    os.environ["ODDSFOX_RUNTIME_ROOT"] = str(root)

    work = Path(tempfile.mkdtemp(prefix="futures-minute-bench-", dir=bench_root))
    compression_label = (
        "uncompressed" if args.compression == "none" else args.compression
    )
    configs = list(MATRIX) if args.matrix else [(args.shard_rows, compression_label)]

    try:
        if args.tier in _STREAM_TIERS:
            comparisons = [
                _run_streamed_pair(
                    tokens=tokens,
                    rows_per_token=rows_per_token,
                    work=work,
                    shard_rows=shard_rows,
                    compression_label=label,
                )
                for shard_rows, label in configs
            ]
        else:
            results = _synthetic_results(tokens=tokens, rows_per_token=rows_per_token)
            comparisons = [
                _compare_one_results(
                    results=results,
                    work=work,
                    shard_rows=shard_rows,
                    compression_label=label,
                )
                for shard_rows, label in configs
            ]
    finally:
        shutil.rmtree(work, ignore_errors=True)

    equal = all(
        item["equality"]["raw_identical"] and item["equality"]["audit_identical"]
        for item in comparisons
    )
    primary = comparisons[0]
    best = max(
        comparisons,
        key=lambda item: float(item["speed_ratio_baseline_over_candidate"]),
    )
    report = {
        "benchmark_version": 2,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tier": args.tier,
        "tokens": tokens,
        "rows_per_token": rows_per_token,
        "total_rows": tokens * rows_per_token,
        "matrix": bool(args.matrix),
        "streamed_input": args.tier in _STREAM_TIERS,
        "comparisons": comparisons,
        "primary": primary,
        "fastest_correct": best if equal else None,
        "equality_all": equal,
        "speed_ratio_baseline_over_candidate": primary[
            "speed_ratio_baseline_over_candidate"
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        f"tier={args.tier} rows={report['total_rows']} equal={equal} "
        f"speedup={report['speed_ratio_baseline_over_candidate']:.2f}x "
        f"report={args.output}"
    )
    if not equal:
        return 2
    if (
        args.require_speedup
        and float(report["speed_ratio_baseline_over_candidate"]) < args.require_speedup
    ):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
