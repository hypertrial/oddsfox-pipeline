#!/usr/bin/env python3
"""Validate a disposable Polymarket minute-odds live-smoke warehouse."""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from oddsfox_pipeline.config.settings import (
    BASE_DIR,
    POLYMARKET_WC2026_MINUTE_ODDS_SMOKE_FRACTION,
    POLYMARKET_WC2026_MINUTE_ODDS_SMOKE_FUTURES_WINDOW_HOURS,
)


def _latest_run_id(conn: duckdb.DuckDBPyConnection, audit_table: str) -> str:
    row = conn.execute(
        f"""
        select fetch_run_id
        from {audit_table}
        order by fetch_finished_at desc
        limit 1
        """
    ).fetchone()
    if row is None:
        raise AssertionError(f"no rows in {audit_table}")
    return str(row[0])


def _audit_counts(
    conn: duckdb.DuckDBPyConnection, *, table: str, fetch_run_id: str
) -> dict[str, int]:
    row = conn.execute(
        f"""
        select
            count(*) as audited,
            count(*) filter (where fetch_status = 'success') as success,
            count(*) filter (where fetch_status = 'empty') as empty,
            count(*) filter (where fetch_status in ('error', 'cancelled')) as hard,
            count(*) filter (where raw_published) as published,
            count(distinct market_id) as markets,
            count(distinct "clobTokenId") as tokens
        from {table}
        where fetch_run_id = ?
        """,
        [fetch_run_id],
    ).fetchone()
    assert row is not None
    return {
        "audited": int(row[0]),
        "success": int(row[1]),
        "empty": int(row[2]),
        "hard": int(row[3]),
        "published": int(row[4]),
        "markets": int(row[5]),
        "tokens": int(row[6]),
    }


def _assert_raw_table(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, int]:
    rows = int(conn.execute(f"select count(*) from {table}").fetchone()[0])
    if rows < 1:
        raise AssertionError(f"{table} is empty")
    fidelity = conn.execute(
        f"select count(*) from {table} where fidelity_minutes <> 1"
    ).fetchone()[0]
    if int(fidelity) != 0:
        raise AssertionError(f"{table} has non-1 fidelity rows: {fidelity}")
    dupes = conn.execute(
        f"""
        select count(*) from (
            select "clobTokenId", timestamp, count(*) as n
            from {table}
            group by 1, 2
            having count(*) > 1
        )
        """
    ).fetchone()[0]
    if int(dupes) != 0:
        raise AssertionError(f"{table} has duplicate PK keys: {dupes}")
    constraints = {
        str(row[0]).upper()
        for row in conn.execute(
            """
            select constraint_type
            from duckdb_constraints()
            where table_name = ?
            """,
            [table.split(".")[-1].strip('"')],
        ).fetchall()
    }
    if "PRIMARY KEY" not in constraints:
        raise AssertionError(f"{table} missing PRIMARY KEY")
    if "CHECK" not in constraints:
        raise AssertionError(f"{table} missing fidelity CHECK")
    markets = int(
        conn.execute(f'select count(distinct market_id) from {table}').fetchone()[0]
    )
    tokens = int(
        conn.execute(f'select count(distinct "clobTokenId") from {table}').fetchone()[0]
    )
    return {"rows": rows, "markets": markets, "tokens": tokens}


def _expected_selected(population: int, fraction: float) -> int:
    return max(1, math.ceil(population * fraction))


def _sync_metrics(conn: duckdb.DuckDBPyConnection, task: str) -> dict[str, Any]:
    row = conn.execute(
        """
        select metrics_json
        from polymarket_wc2026_ops.sync_run_metrics
        where task_name = ?
        """,
        [task],
    ).fetchone()
    if row is None or row[0] is None:
        raise AssertionError(f"missing sync_run_metrics for {task}")
    payload = json.loads(str(row[0]))
    if not isinstance(payload, dict):
        raise AssertionError(f"invalid sync_run_metrics payload for {task}")
    return payload


def validate(
    duckdb_path: Path,
    *,
    fraction: float,
    futures_window_hours: int,
) -> dict[str, Any]:
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        match_metrics = _sync_metrics(conn, "match_minute_odds")
        futures_metrics = _sync_metrics(conn, "futures_minute_odds")
        match_run = _latest_run_id(
            conn, "polymarket_wc2026_ops.match_minute_odds_fetch_audit"
        )
        futures_run = _latest_run_id(
            conn, "polymarket_wc2026_ops.futures_minute_odds_fetch_audit"
        )
        if match_metrics.get("fetch_run_id") != match_run:
            raise AssertionError(
                "match metrics fetch_run_id does not match latest audit run"
            )
        if futures_metrics.get("fetch_run_id") != futures_run:
            raise AssertionError(
                "futures metrics fetch_run_id does not match latest audit run"
            )
        match_audit = _audit_counts(
            conn,
            table="polymarket_wc2026_ops.match_minute_odds_fetch_audit",
            fetch_run_id=match_run,
        )
        futures_audit = _audit_counts(
            conn,
            table="polymarket_wc2026_ops.futures_minute_odds_fetch_audit",
            fetch_run_id=futures_run,
        )

        if match_audit["hard"] != 0 or match_audit["empty"] != 0:
            raise AssertionError(f"match audit not all-success: {match_audit}")
        if match_audit["published"] != match_audit["success"]:
            raise AssertionError(f"match published mismatch: {match_audit}")
        if match_audit["success"] < 1:
            raise AssertionError("match smoke published zero success tokens")

        if futures_audit["hard"] != 0:
            raise AssertionError(f"futures audit has hard failures: {futures_audit}")
        if futures_audit["success"] < 1:
            raise AssertionError("futures smoke published zero success tokens")
        if futures_audit["published"] != futures_audit["success"]:
            raise AssertionError(f"futures published mismatch: {futures_audit}")

        match_raw = _assert_raw_table(
            conn, "polymarket_wc2026_raw.match_minute_odds_history"
        )
        futures_raw = _assert_raw_table(
            conn, "polymarket_wc2026_raw.futures_minute_odds_history"
        )

        for label, metrics, audit in (
            ("match", match_metrics, match_audit),
            ("futures", futures_metrics, futures_audit),
        ):
            if not metrics.get("sample_enabled"):
                raise AssertionError(f"{label} smoke metrics missing sample_enabled")
            population = int(metrics["population_markets"])
            selected = int(metrics["selected_markets"])
            expected = _expected_selected(population, float(metrics["sample_fraction"]))
            if selected != expected:
                raise AssertionError(
                    f"{label} selected markets {selected} != expected {expected} "
                    f"(population={population})"
                )
            if abs(float(metrics["sample_fraction"]) - fraction) > 1e-9:
                raise AssertionError(
                    f"{label} sample_fraction {metrics['sample_fraction']} != {fraction}"
                )
            if audit["markets"] != selected and label == "match":
                raise AssertionError(
                    f"match audit markets {audit['markets']} != selected {selected}"
                )
            if label == "futures" and audit["markets"] != selected:
                raise AssertionError(
                    f"futures audit markets {audit['markets']} != selected {selected}"
                )
            if int(metrics["selected_tokens"]) != audit["tokens"]:
                raise AssertionError(
                    f"{label} selected_tokens {metrics['selected_tokens']} "
                    f"!= audit tokens {audit['tokens']}"
                )

        if match_raw["markets"] != match_audit["markets"]:
            raise AssertionError(
                "match raw markets "
                f"{match_raw['markets']} != audit {match_audit['markets']}"
            )
        if match_raw["tokens"] != match_audit["tokens"]:
            raise AssertionError(
                "match raw tokens do not match audit tokens: "
                f"{match_raw['tokens']} vs {match_audit['tokens']}"
            )
        if match_raw["tokens"] != match_raw["markets"] * 2:
            raise AssertionError(
                "match smoke must keep two tokens per selected market: "
                f"markets={match_raw['markets']} tokens={match_raw['tokens']}"
            )

        published_futures_markets = int(
            conn.execute(
                """
                select count(distinct market_id)
                from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
                where fetch_run_id = ? and raw_published
                """,
                [futures_run],
            ).fetchone()[0]
        )
        if futures_raw["markets"] != published_futures_markets:
            raise AssertionError(
                "futures raw markets "
                f"{futures_raw['markets']} != published {published_futures_markets}"
            )
        if futures_metrics.get("sample_window_hours") is None:
            raise AssertionError("futures smoke missing sample_window_hours")
        window_hours = int(futures_metrics["sample_window_hours"])
        if window_hours != int(futures_window_hours):
            raise AssertionError(
                f"futures sample_window_hours {window_hours} "
                f"!= expected {futures_window_hours}"
            )
        if window_hours < 1:
            raise AssertionError(
                f"invalid futures sample_window_hours={window_hours}"
            )
        oversize = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_ops.futures_minute_odds_fetch_audit
            where fetch_run_id = ?
              and epoch(exact_window_end_at)
                - epoch(exact_window_start_at) > (? * 3600) + 1
            """,
            [futures_run, window_hours],
        ).fetchone()[0]
        if int(oversize) != 0:
            raise AssertionError(
                f"futures audit has {oversize} windows longer than "
                f"{window_hours}h tail cap"
            )

        mart = conn.execute(
            """
            select
                count(*) as mart_rows,
                count(*) filter (where minute_source = 'match') as match_rows,
                count(*) filter (where minute_source = 'futures') as futures_rows,
                count(*) filter (
                    where open_odds is null or high_odds is null
                       or low_odds is null or close_odds is null
                ) as null_ohlc,
                count(*) filter (
                    where not (low_odds <= open_odds and open_odds <= high_odds)
                       or not (low_odds <= close_odds and close_odds <= high_odds)
                ) as ohlc_order_issues,
                count(*) - count(distinct (odds_minute_epoch, market_id, clob_token_id))
                    as duplicate_grain
            from polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
            """
        ).fetchone()
        assert mart is not None
        if int(mart[0]) < 1 or int(mart[1]) < 1 or int(mart[2]) < 1:
            raise AssertionError(f"unified mart missing sources: {mart}")
        if int(mart[3]) != 0 or int(mart[4]) != 0 or int(mart[5]) != 0:
            raise AssertionError(f"unified mart OHLC/grain issues: {mart}")

        dq = conn.execute(
            """
            select
                has_match_rows,
                has_futures_rows,
                futures_audit_healthy,
                blocking_issue_keys
            from polymarket_wc2026_observability.polymarket_wc2026_market_minute_odds_data_quality
            """
        ).fetchone()
        if dq != (True, True, True, None):
            raise AssertionError(f"unified DQ failed: {dq}")

        report = {
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "duckdb_path": str(duckdb_path),
            "sample_fraction": fraction,
            "match_fetch_run_id": match_run,
            "futures_fetch_run_id": futures_run,
            "match_metrics": {
                key: match_metrics.get(key)
                for key in (
                    "sample_enabled",
                    "sample_fraction",
                    "sample_seed",
                    "population_markets",
                    "selected_markets",
                    "selected_tokens",
                    "selected_market_ids_sha256",
                )
            },
            "futures_metrics": {
                key: futures_metrics.get(key)
                for key in (
                    "sample_enabled",
                    "sample_fraction",
                    "sample_seed",
                    "sample_window_hours",
                    "population_markets",
                    "selected_markets",
                    "selected_tokens",
                    "selected_market_ids_sha256",
                )
            },
            "match_audit": match_audit,
            "futures_audit": futures_audit,
            "match_raw": match_raw,
            "futures_raw": futures_raw,
            "mart": {
                "rows": int(mart[0]),
                "match_rows": int(mart[1]),
                "futures_rows": int(mart[2]),
            },
            "blocking_issue_keys": None,
            "status": "ok",
        }
        return report
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument(
        "--fraction",
        type=float,
        default=POLYMARKET_WC2026_MINUTE_ODDS_SMOKE_FRACTION,
    )
    parser.add_argument(
        "--futures-window-hours",
        type=int,
        default=POLYMARKET_WC2026_MINUTE_ODDS_SMOKE_FUTURES_WINDOW_HOURS,
    )
    parser.add_argument(
        "--report-path",
        default="",
        help="Optional JSON report path under runtime cache",
    )
    args = parser.parse_args(argv)
    duckdb_path = Path(args.duckdb_path).expanduser().resolve()
    if not duckdb_path.is_file():
        raise SystemExit(f"warehouse missing: {duckdb_path}")
    report = validate(
        duckdb_path,
        fraction=float(args.fraction),
        futures_window_hours=int(args.futures_window_hours),
    )
    report_path = (
        Path(args.report_path).expanduser()
        if args.report_path
        else (
            Path(BASE_DIR)
            / ".cache"
            / "runtime"
            / "smoke"
            / "minute-odds"
            / f"{duckdb_path.stem}.json"
        )
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True))
    print(f"wrote {report_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
