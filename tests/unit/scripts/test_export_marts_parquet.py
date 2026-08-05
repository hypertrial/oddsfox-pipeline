"""Tests for scripts/export_marts_parquet.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import export_marts_parquet as mod

    return mod


def test_export_all_marts_writes_one_parquet_per_table(tmp_path: Path) -> None:
    mod = _load_export_module()
    conn = duckdb.connect()
    try:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute("create schema kalshi_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds (
                market_id varchar,
                odds_hour_epoch bigint
            )
            """
        )
        conn.execute(
            """
            insert into polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
            values ('m1', 1), ('m2', 2)
            """
        )
        # Kalshi marts are dbt views in production; export must include them.
        conn.execute(
            """
            create view kalshi_wc2026_marts.kalshi_wc2026_stage_markets as
            select 'T1' as market_ticker
            """
        )
        # Staging/non-mart schemas must be ignored.
        conn.execute("create schema polymarket_wc2026_staging")
        conn.execute("create table polymarket_wc2026_staging.stg_markets (id varchar)")

        out = tmp_path / "exports"
        results = mod.export_all_marts(conn, out)
        assert [(s, n, r) for s, n, _, r in results] == [
            ("kalshi_wc2026_marts", "kalshi_wc2026_stage_markets", 1),
            (
                "polymarket_wc2026_marts",
                "polymarket_wc2026_market_hourly_odds",
                2,
            ),
        ]
        hourly = (
            out / "polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds.parquet"
        )
        stage = out / "kalshi_wc2026_marts.kalshi_wc2026_stage_markets.parquet"
        assert hourly.is_file() and stage.is_file()
        assert conn.execute(
            "select count(*) from read_parquet(?)", [str(hourly)]
        ).fetchone() == (2,)
    finally:
        conn.close()


def test_export_all_marts_raises_when_empty(tmp_path: Path) -> None:
    mod = _load_export_module()
    conn = duckdb.connect()
    try:
        try:
            mod.export_all_marts(conn, tmp_path)
            raise AssertionError("expected LookupError")
        except LookupError as exc:
            assert "No mart tables found" in str(exc)
    finally:
        conn.close()
