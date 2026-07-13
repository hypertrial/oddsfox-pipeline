#!/usr/bin/env python3
"""Run local Great Expectations-style checks against the dbt build database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb
import great_expectations as gx

PUBLIC_RELATIONS: tuple[tuple[str, str, tuple[str, ...], tuple[str, ...], int], ...] = (
    (
        "wc2026_marts",
        "wc2026_knockout_match_hourly_odds",
        ("fifa_match_id", "odds_hour_epoch"),
        (
            "fifa_match_id",
            "odds_hour_epoch",
            "polymarket_home_advance_price",
            "polymarket_away_advance_price",
            "kalshi_home_advance_price",
            "kalshi_away_advance_price",
        ),
        0,
    ),
    (
        "international_results_wc2026_marts",
        "international_results_wc2026_team_status",
        ("team_name",),
        ("team_name", "tournament_status", "is_still_alive"),
        0,
    ),
    (
        "polymarket_wc2026_marts",
        "polymarket_wc2026_knockout_token_hourly_odds",
        ("clob_token_id", "odds_hour_epoch"),
        ("clob_token_id", "odds_hour_epoch", "close_price"),
        0,
    ),
    (
        "polymarket_us_midterms_2026_marts",
        "polymarket_us_midterms_2026_market_token_hourly_odds",
        ("clob_token_id", "odds_hour_epoch"),
        ("clob_token_id", "odds_hour_epoch", "close_price"),
        0,
    ),
    (
        "kalshi_wc2026_marts",
        "kalshi_wc2026_stage_market_hourly_odds",
        ("market_ticker", "odds_hour_epoch"),
        ("market_ticker", "odds_hour_epoch", "progression_close_price"),
        0,
    ),
    (
        "kalshi_wc2026_marts",
        "kalshi_wc2026_group_winner_market_hourly_odds",
        ("market_ticker", "odds_hour_epoch"),
        ("market_ticker", "odds_hour_epoch", "close_price"),
        0,
    ),
)


def _relation_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [schema, table],
        ).fetchone()[0]
    )


def _columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> set[str]:
    rows = conn.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = ? and table_name = ?
        """,
        [schema, table],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _count(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    return int(conn.execute(f'select count(*) from "{schema}"."{table}"').fetchone()[0])


def _duplicate_count(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    grain: tuple[str, ...],
) -> int:
    cols = ", ".join(f'"{col}"' for col in grain)
    return int(
        conn.execute(
            f"""
            select count(*)
            from (
                select {cols}
                from "{schema}"."{table}"
                group by {cols}
                having count(*) > 1
            )
            """
        ).fetchone()[0]
    )


def _price_out_of_range(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    price_columns: tuple[str, ...],
) -> int:
    if not price_columns:
        return 0
    clauses = " or ".join(
        f'("{column}" is not null and ("{column}" < 0 or "{column}" > 1))'
        for column in price_columns
    )
    return int(
        conn.execute(
            f'select count(*) from "{schema}"."{table}" where {clauses}'
        ).fetchone()[0]
    )


def _check_public_relations(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for schema, table, grain, required_columns, minimum_row_count in PUBLIC_RELATIONS:
        check: dict[str, Any] = {"schema": schema, "table": table}
        if not _relation_exists(conn, schema, table):
            check.update({"success": False, "error": "relation missing"})
            checks.append(check)
            continue
        columns = _columns(conn, schema, table)
        missing = sorted(set(required_columns) - columns)
        price_columns = tuple(col for col in required_columns if col.endswith("price"))
        row_count = _count(conn, schema, table)
        duplicate_count = _duplicate_count(conn, schema, table, grain)
        out_of_range = _price_out_of_range(conn, schema, table, price_columns)
        check.update(
            {
                "success": (
                    not missing
                    and row_count >= minimum_row_count
                    and duplicate_count == 0
                    and out_of_range == 0
                ),
                "row_count": row_count,
                "minimum_row_count": minimum_row_count,
                "missing_columns": missing,
                "duplicate_grain_rows": duplicate_count,
                "price_out_of_range_rows": out_of_range,
            }
        )
        checks.append(check)
    return checks


def _check_data_quality_tables(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for schema, table in (
        (
            "international_results_wc2026_observability",
            "international_results_wc2026_data_quality",
        ),
        ("kalshi_wc2026_observability", "kalshi_wc2026_data_quality"),
        ("polymarket_wc2026_observability", "polymarket_wc2026_knockout_data_quality"),
        ("wc2026_observability", "wc2026_knockout_match_odds_data_quality"),
    ):
        if not _relation_exists(conn, schema, table):
            checks.append(
                {
                    "schema": schema,
                    "table": table,
                    "success": False,
                    "error": "relation missing",
                }
            )
            continue
        columns = _columns(conn, schema, table)
        if "severity" not in columns:
            checks.append(
                {
                    "schema": schema,
                    "table": table,
                    "success": True,
                    "error_rows": 0,
                }
            )
            continue
        error_rows = int(
            conn.execute(
                f"""
                select count(*)
                from "{schema}"."{table}"
                where lower(cast(severity as varchar)) = 'error'
                """
            ).fetchone()[0]
        )
        checks.append(
            {
                "schema": schema,
                "table": table,
                "success": error_rows == 0,
                "error_rows": error_rows,
            }
        )
    return checks


def _write_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def _write_docs(report: dict[str, Any], docs_dir: Path) -> None:
    docs_dir.mkdir(parents=True, exist_ok=True)
    rows = "\n".join(
        f"<tr><td>{check.get('schema')}</td><td>{check.get('table')}</td>"
        f"<td>{'pass' if check.get('success') else 'fail'}</td></tr>"
        for check in report["checks"]
    )
    (docs_dir / "index.html").write_text(
        "<!doctype html><title>OddsFox GX Data Quality</title>"
        "<h1>OddsFox GX Data Quality</h1><table>"
        "<tr><th>schema</th><th>table</th><th>status</th></tr>"
        f"{rows}</table>\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument(
        "--report-path",
        default=".cache/gx_data_quality_report.json",
    )
    parser.add_argument(
        "--docs-dir",
        default=".cache/gx_data_docs",
    )
    args = parser.parse_args()

    report_path = Path(args.report_path)
    docs_dir = Path(args.docs_dir)
    with duckdb.connect(args.duckdb_path, read_only=True) as conn:
        checks = _check_public_relations(conn) + _check_data_quality_tables(conn)
    report = {
        "tool": "great_expectations",
        "great_expectations_version": getattr(gx, "__version__", "unknown"),
        "success": all(check["success"] for check in checks),
        "checks": checks,
    }
    _write_report(report, report_path)
    _write_docs(report, docs_dir)
    if not report["success"]:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1
    print(f"Great Expectations data-quality checks passed ({report_path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
