"""Synthetic exactness tests for the optional v3/v4 benchmark tool."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import duckdb
import pytest


def _load_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import benchmark_polymarket_wc2026_polygon_settlement as benchmark

    return importlib.reload(benchmark)


def _database(path: Path, *, v4: bool) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE SCHEMA polymarket_wc2026_raw")
        conn.execute("CREATE SCHEMA polymarket_wc2026_ops")
        conn.execute("CREATE SCHEMA polymarket_wc2026_marts")
        conn.execute("CREATE SCHEMA polymarket_wc2026_observability")
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_raw.polygon_settlement_fills AS
            SELECT
                'scan' AS scan_id,
                10::BIGINT AS chunk_from_block,
                12::BIGINT AS chunk_to_block,
                'tx-economic-locator' AS transaction_hash,
                3::BIGINT AS passive_log_index,
                0::INTEGER AS normalized_leg_ordinal,
                0.6::DECIMAL(38, 18) AS price,
                ? AS decoder_version,
                TIMESTAMP '2026-08-01 00:00:01' AS ingested_at
            """,
            ["polygon-v2-settlement-v4" if v4 else "polygon-v2-settlement-v3"],
        )
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_ops.polygon_settlement_scan_runs AS
            SELECT
                'scan' AS scan_id,
                ? AS normalizer_version,
                repeat('a', 64) AS manifest_sha256,
                'published' AS status,
                TRUE AS raw_published,
                TIMESTAMP '2026-08-01 00:00:00' AS started_at,
                ?::TIMESTAMP AS published_at
            """,
            [
                "polygon-v2-settlement-v4" if v4 else "polygon-v2-settlement-v3",
                "2026-08-01 00:00:02" if v4 else "2026-08-01 00:00:08",
            ],
        )
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_ops.polygon_settlement_scan_chunks AS
            SELECT
                'scan' AS scan_id,
                10::BIGINT AS from_block,
                12::BIGINT AS to_block,
                'success' AS status,
                4::BIGINT AS event_count,
                1::BIGINT AS normalized_fill_count,
                3::BIGINT AS http_request_count,
                1::BIGINT AS log_rpc_call_count,
                1::BIGINT AS receipt_rpc_call_count,
                1::BIGINT AS header_rpc_call_count,
                0::BIGINT AS retry_count,
                0::BIGINT AS adaptive_split_count
            """
        )
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds AS
            SELECT range AS elapsed_window_minute, 0.5::DECIMAL(38, 18) AS yes_close
            FROM range(39120)
            """
        )
        conn.execute(
            """
            CREATE TABLE polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_data_quality AS
            SELECT TRUE AS publication_ready, NULL::VARCHAR AS blocking_issue_keys
            """
        )


def test_benchmark_requires_exact_fills_mart_and_sanitizes_report(tmp_path) -> None:
    benchmark = _load_module()
    v3 = tmp_path / "v3.duckdb"
    v4 = tmp_path / "v4.duckdb"
    output = tmp_path / "benchmark.json"
    _database(v3, v4=False)
    _database(v4, v4=True)

    report = benchmark.compare_polygon_benchmarks(v3, v4, output)
    assert report["speed_ratio_v3_over_v4"] == 4
    assert report["equality"] == {
        "economic_fill_differences": 0,
        "mart_differences": 0,
        "mart_rows": 39_120,
    }
    serialized = output.read_text(encoding="utf-8")
    assert json.loads(serialized) == report
    assert not any(
        prohibited in serialized.casefold()
        for prohibited in ("rpc_url", "wallet", "raw_topics", str(tmp_path).casefold())
    )

    with duckdb.connect(str(v4)) as conn:
        conn.execute(
            "UPDATE polymarket_wc2026_raw.polygon_settlement_fills SET price = 0.7"
        )
    with pytest.raises(RuntimeError, match="economic fills differ"):
        benchmark.compare_polygon_benchmarks(v3, v4, output)

    with duckdb.connect(str(v4)) as conn:
        conn.execute(
            "UPDATE polymarket_wc2026_raw.polygon_settlement_fills SET price = 0.6"
        )
        conn.execute(
            """
            UPDATE polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds
            SET yes_close = 0.4 WHERE elapsed_window_minute = 1
            """
        )
    with pytest.raises(RuntimeError, match="marts differ"):
        benchmark.compare_polygon_benchmarks(v3, v4, output)


def test_benchmark_refuses_missing_same_and_unpublished_inputs(tmp_path) -> None:
    benchmark = _load_module()
    missing = tmp_path / "missing.duckdb"
    with pytest.raises(ValueError, match="different files"):
        benchmark.compare_polygon_benchmarks(missing, missing, tmp_path / "out.json")
    with pytest.raises(FileNotFoundError, match="Both completed"):
        benchmark.compare_polygon_benchmarks(
            missing, tmp_path / "also-missing.duckdb", tmp_path / "out.json"
        )

    v3 = tmp_path / "v3.duckdb"
    v4 = tmp_path / "v4.duckdb"
    _database(v3, v4=False)
    _database(v4, v4=True)
    with duckdb.connect(str(v3)) as conn:
        conn.execute(
            "UPDATE polymarket_wc2026_ops.polygon_settlement_scan_runs "
            "SET status = 'failed', raw_published = FALSE"
        )
    with pytest.raises(RuntimeError, match="exactly one published scan"):
        benchmark.compare_polygon_benchmarks(v3, v4, tmp_path / "out.json")
