#!/usr/bin/env python3
"""Benchmark the unified minute-odds dbt graph on disposable synthetic data.

Seeds a disposable DuckDB under ``${ODDSFOX_RUNTIME_ROOT}/benchmarks/minute-odds-dbt/``,
runs ``+polymarket_wc2026_market_minute_odds_data_quality``, and writes a JSON
report with wall time, output rows, DQ blockers, peak RSS, and peak DuckDB temp
bytes. Never opens the operator warehouse.

Tiers scale the futures raw leg (all tokens retained; mart is primary-only):
  smoke: 8 markets x 64 rows/token
  tune: 40 x 50_000 (~2M primary rows)
  performance: 200 x 50_000 (~10M) — default timing tier
  production-shaped: 60_000 x 6_284 (~377M) — opt-in acceptance
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from _bootstrap import ensure_src_on_path  # noqa: E402

ensure_src_on_path()
sys.path.insert(0, str(REPO_ROOT))

from tests.integration.match_minute_seed import (  # noqa: E402
    seed_match_minute_contract,
    seed_wc2026_schedule_matches,
)

import oddsfox_pipeline.storage.duckdb.connection as connection  # noqa: E402
from oddsfox_pipeline.storage.minute_odds_snapshots import (  # noqa: E402
    backfill_primary_ohlc_table,
)

TIER_SIZES = {
    "smoke": (8, 64),
    "tune": (40, 50_000),
    "performance": (200, 50_000),
    "production-shaped": (60_000, 6_284),
}


def _peak_rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if os.uname().sysname == "Darwin":
        return int(usage)
    return int(usage) * 1024


def _dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            total += (Path(root) / name).stat().st_size
    return total


def _seed_futures(
    conn: duckdb.DuckDBPyConnection, *, tokens: int, rows_per_token: int
) -> dict:
    # Raw minute tables store TIMESTAMP (UTC wall, no zone). Seed with
    # to_timestamp(...) AT TIME ZONE 'UTC' so non-UTC sessions do not shift windows.
    start = datetime(2026, 6, 11, tzinfo=timezone.utc)
    end = datetime(2026, 7, 19, tzinfo=timezone.utc)
    observed = datetime(2026, 8, 1, tzinfo=timezone.utc)
    base_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    observed_ts = int(observed.timestamp())
    conn.execute(
        f"""
        INSERT INTO polymarket_wc2026_raw.markets (
            id, question, category, description, outcomes, volume, active, closed,
            created_at, scraped_at, end_date, slug, event_slug, event_id,
            event_title, condition_id, sports_market_type, group_item_title,
            clob_token_ids, is_resolved, tags
        )
        SELECT
            printf('fut-market-%05d', i),
            'Winner?',
            'sports',
            '',
            '["Yes", "No"]',
            200000,
            false,
            true,
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            printf('fut-market-%05d', i),
            printf('fut-event-%05d', i),
            printf('fut-event-%05d', i),
            'WC Winner',
            printf('fut-condition-%05d', i),
            'tournament_winner',
            'Winner',
            printf('["fut-token-%05d-yes", "fut-token-%05d-no"]', i, i),
            false,
            '[]'
        FROM range({tokens}) AS t(i)
        """,
        [base_ts, observed_ts, end_ts],
    )
    conn.execute(
        f"""
        INSERT INTO polymarket_wc2026_ops.market_scope_registry (
            scope_name, market_id, event_slug, event_id, source, refreshed_at,
            event_volume_usd_lifetime_reported, is_event_volume_eligible,
            first_eligible_at
        )
        SELECT
            'wc2026',
            printf('fut-market-%05d', i),
            printf('fut-event-%05d', i),
            printf('fut-event-%05d', i),
            'benchmark',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            200000,
            true,
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC'
        FROM range({tokens}) AS t(i)
        """,
        [observed_ts, base_ts],
    )
    # Match-minute seed markets also need registry rows for the unified mart.
    conn.execute(
        """
        INSERT INTO polymarket_wc2026_ops.market_scope_registry (
            scope_name, market_id, event_slug, event_id, source, refreshed_at,
            event_volume_usd_lifetime_reported, is_event_volume_eligible,
            first_eligible_at
        )
        SELECT
            'wc2026',
            m.id,
            m.event_slug,
            m.event_id,
            'benchmark',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            coalesce(m.volume, 1000.0),
            true,
            coalesce(
                m.created_at,
                to_timestamp(?::BIGINT) AT TIME ZONE 'UTC'
            )
        FROM polymarket_wc2026_raw.markets AS m
        WHERE m.id NOT LIKE 'fut-market-%'
          AND NOT EXISTS (
              SELECT 1
              FROM polymarket_wc2026_ops.market_scope_registry AS r
              WHERE r.scope_name = 'wc2026' AND r.market_id = m.id
          )
        """,
        [observed_ts, base_ts],
    )
    conn.execute(
        f"""
        INSERT INTO polymarket_wc2026_raw.event_market_payload_snapshots (
            market_id, question, category, description, outcomes, volume,
            active, closed, created_at, scraped_at, end_date, slug, event_slug,
            event_id, event_title, condition_id, sports_market_type,
            group_item_title, clob_token_ids, is_resolved, tags, observed_at
        )
        SELECT
            printf('fut-market-%05d', i),
            'Winner?',
            'sports',
            '',
            '["Yes", "No"]',
            200000,
            false,
            true,
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            printf('fut-market-%05d', i),
            printf('fut-event-%05d', i),
            printf('fut-event-%05d', i),
            'WC Winner',
            printf('fut-condition-%05d', i),
            'tournament_winner',
            'Winner',
            printf('["fut-token-%05d-yes", "fut-token-%05d-no"]', i, i),
            false,
            '[]',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC'
        FROM range({tokens}) AS t(i)
        """,
        [base_ts, observed_ts, end_ts, observed_ts],
    )
    conn.execute(
        f"""
        INSERT INTO polymarket_wc2026_raw.futures_minute_odds_history (
            market_id, clobTokenId, timestamp, price, fidelity_minutes,
            window_start_at, window_end_at, ingested_at
        )
        SELECT
            printf('fut-market-%05d', token_idx),
            printf('fut-token-%05d-%s', token_idx, side),
            (?::BIGINT + row_idx * 60),
            (0.1 + ((row_idx + token_idx) % 80) / 100.0)::DOUBLE,
            1,
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC'
        FROM range({tokens}) AS t(token_idx)
        CROSS JOIN (SELECT * FROM (VALUES ('yes'), ('no')) AS s(side))
        CROSS JOIN range({rows_per_token}) AS r(row_idx)
        """,
        [base_ts, base_ts, end_ts, observed_ts],
    )
    conn.execute(
        f"""
        INSERT INTO polymarket_wc2026_ops.futures_minute_odds_fetch_audit (
            fetch_run_id, market_id, clobTokenId, fetch_status, raw_published,
            fidelity_minutes, exact_window_start_at, exact_window_end_at,
            request_start_epoch, request_end_epoch, source_row_count,
            window_row_count, window_history_sha256, source_endpoint,
            fetch_started_at, fetch_finished_at
        )
        SELECT
            'bench-futures-minute',
            printf('fut-market-%05d', token_idx),
            printf('fut-token-%05d-%s', token_idx, side),
            'success',
            true,
            1,
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            ?::BIGINT,
            ?::BIGINT + {rows_per_token} * 60,
            {rows_per_token},
            {rows_per_token},
            repeat('a', 64),
            'https://clob.polymarket.com/prices-history',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC',
            to_timestamp(?::BIGINT) AT TIME ZONE 'UTC'
        FROM range({tokens}) AS t(token_idx)
        CROSS JOIN (SELECT * FROM (VALUES ('yes'), ('no')) AS s(side))
        """,
        [base_ts, end_ts, base_ts, base_ts, observed_ts, observed_ts],
    )
    raw_rows = int(
        conn.execute(
            "SELECT count(*) FROM polymarket_wc2026_raw.futures_minute_odds_history"
        ).fetchone()[0]
    )
    return {
        "futures_markets": tokens,
        "raw_tokens": tokens * 2,
        "raw_rows": raw_rows,
        "rows_per_token": rows_per_token,
    }


def _write_profile(
    profiles_dir: Path,
    db_path: Path,
    threads: int,
    temp_dir: Path,
    *,
    memory_limit: str = "20GB",
) -> None:
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{db_path.as_posix()}"
      schema: dbt
      threads: {threads}
      settings:
        temp_directory: "{temp_dir.as_posix()}"
        memory_limit: "{memory_limit}"
        preserve_insertion_order: false
""".lstrip(),
        encoding="utf-8",
    )


def _git_sha() -> str | None:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return (proc.stdout or "").strip() or None


def _child_peak_rss_bytes(pid: int) -> int | None:
    # Darwin: ps rss is in KB.
    try:
        proc = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    text = (proc.stdout or "").strip()
    if not text:
        return None
    try:
        return int(text) * 1024
    except ValueError:
        return None


def _run_dbt(db_path: Path, *, profiles_dir: Path, target_dir: Path) -> dict:
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    env["DBT_TARGET_PATH"] = str(target_dir)
    env["DBT_THREADS"] = env.get("DBT_THREADS", "2")
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "build",
        "--project-dir",
        str(REPO_ROOT / "dbt"),
        "--profiles-dir",
        str(profiles_dir),
        "--select",
        "+polymarket_wc2026_market_minute_odds_data_quality",
        "--exclude",
        "tag:polygon_settlement tag:pmxt_order_book resource_type:seed",
    ]
    started = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    peak_child_rss = 0
    assert proc.stdout is not None
    assert proc.stderr is not None
    # Sample child RSS while dbt runs; cheap enough for smoke/performance tiers.
    while proc.poll() is None:
        sample = _child_peak_rss_bytes(proc.pid)
        if sample:
            peak_child_rss = max(peak_child_rss, sample)
        time.sleep(0.25)
    stdout, stderr = proc.communicate()
    sample = _child_peak_rss_bytes(proc.pid)
    if sample:
        peak_child_rss = max(peak_child_rss, sample)
    elapsed = time.perf_counter() - started
    return {
        "returncode": proc.returncode,
        "elapsed_seconds": round(elapsed, 3),
        "stdout_tail": (stdout or "")[-4000:],
        "stderr_tail": (stderr or "")[-2000:],
        "child_peak_rss_bytes": peak_child_rss,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tier",
        choices=sorted(TIER_SIZES),
        default="performance",
        help="Synthetic scale tier (default: performance ~10M primary rows)",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-limit", default="20GB")
    args = parser.parse_args(argv)

    tokens, rows_per_token = TIER_SIZES[args.tier]
    runtime_root = Path(
        os.environ.get("ODDSFOX_RUNTIME_ROOT", REPO_ROOT / ".cache" / "runtime")
    ).resolve()
    bench_root = runtime_root / "benchmarks" / "minute-odds-dbt" / args.tier
    if bench_root.exists():
        shutil.rmtree(bench_root)
    bench_root.mkdir(parents=True, exist_ok=True)
    db_path = bench_root / "warehouse.duckdb"
    profiles_dir = bench_root / "profiles"
    target_dir = bench_root / "dbt-target"
    temp_dir = bench_root / "duckdb-temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    os.environ["ODDSFOX_RUNTIME_ROOT"] = str(runtime_root)
    os.environ["DUCKDB_PATH"] = str(db_path)
    os.environ["DUCKDB_NAME"] = str(db_path)

    seed_started = time.perf_counter()
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        seed_match_minute_contract(conn)
        futures_stats = _seed_futures(
            conn, tokens=tokens, rows_per_token=rows_per_token
        )
        primary_ids = {f"fut-token-{idx:05d}-yes" for idx in range(tokens)}
        backfill_primary_ohlc_table(
            conn, leg="futures", primary_token_ids=primary_ids
        )
        backfill_primary_ohlc_table(conn, leg="match")
    seed_seconds = round(time.perf_counter() - seed_started, 3)
    _write_profile(
        profiles_dir,
        db_path,
        args.threads,
        temp_dir,
        memory_limit=args.memory_limit,
    )

    # dbt seed + schedule overlay (same path as integration tests).
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DBT_TARGET_PATH"] = str(target_dir)
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    seed_proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "seed",
            "--exclude",
            "tag:polygon_settlement tag:pmxt_order_book",
            "--project-dir",
            str(REPO_ROOT / "dbt"),
            "--profiles-dir",
            str(profiles_dir),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
    )
    if seed_proc.returncode != 0:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(
                {
                    "tier": args.tier,
                    "error": "dbt seed failed",
                    "stdout_tail": (seed_proc.stdout or "")[-2000:],
                    "stderr_tail": (seed_proc.stderr or "")[-2000:],
                },
                indent=2,
            )
            + "\n"
        )
        return seed_proc.returncode
    with duckdb.connect(str(db_path)) as conn:
        seed_wc2026_schedule_matches(conn)

    rss_before = _peak_rss_bytes()
    dbt_result = _run_dbt(db_path, profiles_dir=profiles_dir, target_dir=target_dir)
    temp_bytes = _dir_bytes(temp_dir) + _dir_bytes(Path(str(db_path) + ".tmp"))

    report: dict = {
        "tier": args.tier,
        "git_sha": _git_sha(),
        "futures_markets": tokens,
        "rows_per_token": rows_per_token,
        "seed": futures_stats,
        "seed_seconds": seed_seconds,
        "duckdb_settings": {
            "threads": args.threads,
            "memory_limit": args.memory_limit,
            "temp_directory": str(temp_dir),
            "preserve_insertion_order": False,
        },
        "dbt": {
            "returncode": dbt_result["returncode"],
            "elapsed_seconds": dbt_result["elapsed_seconds"],
            "child_peak_rss_bytes": dbt_result.get("child_peak_rss_bytes", 0),
        },
        "peak_rss_bytes": _peak_rss_bytes(),
        "rss_delta_bytes": max(0, _peak_rss_bytes() - rss_before),
        "peak_duckdb_temp_bytes": temp_bytes,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    if dbt_result["returncode"] == 0:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            mart_rows = int(
                conn.execute(
                    """
                    SELECT count(*)
                    FROM polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
                    """
                ).fetchone()[0]
            )
            futures_tokens = int(
                conn.execute(
                    """
                    SELECT count(DISTINCT clob_token_id)
                    FROM polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
                    WHERE minute_source = 'futures'
                    """
                ).fetchone()[0]
            )
            raw_tokens = int(
                conn.execute(
                    """
                    SELECT count(DISTINCT clobTokenId)
                    FROM polymarket_wc2026_raw.futures_minute_odds_history
                    """
                ).fetchone()[0]
            )
            dq = conn.execute(
                """
                SELECT has_match_rows, has_futures_rows, blocking_issue_keys,
                       futures_primary_tokens_with_prices
                FROM polymarket_wc2026_observability.polymarket_wc2026_market_minute_odds_data_quality
                """
            ).fetchone()
            report["mart_rows"] = mart_rows
            report["mart_futures_tokens"] = futures_tokens
            report["raw_futures_tokens"] = raw_tokens
            report["dq"] = {
                "has_match_rows": bool(dq[0]),
                "has_futures_rows": bool(dq[1]),
                "blocking_issue_keys": dq[2],
                "futures_primary_tokens_with_prices": int(dq[3]),
            }
            report["primary_token_ok"] = futures_tokens == tokens
            report["all_token_raw_ok"] = raw_tokens == tokens * 2
        finally:
            conn.close()
    else:
        report["dbt"]["stdout_tail"] = dbt_result["stdout_tail"]
        report["dbt"]["stderr_tail"] = dbt_result["stderr_tail"]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    if dbt_result["returncode"] != 0:
        return dbt_result["returncode"]
    if report.get("dq", {}).get("blocking_issue_keys") is not None:
        return 2
    if not report.get("primary_token_ok", False):
        return 3
    if not report.get("all_token_raw_ok", False):
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
