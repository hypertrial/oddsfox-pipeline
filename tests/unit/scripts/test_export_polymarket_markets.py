"""Tests for scripts/export_polymarket_markets.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_polymarket_markets import (
        export_all_polymarket_markets_catalogs,
        export_polymarket_markets_catalog,
        mart_exists,
    )

    return (
        export_polymarket_markets_catalog,
        export_all_polymarket_markets_catalogs,
        mart_exists,
    )


def test_export_polymarket_markets_catalog_round_trip(tmp_path: Path) -> None:
    export_one, export_all, mart_exists = _load_export_module()
    conn = duckdb.connect()
    try:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute("create schema polymarket_us_midterms_2026_marts")
        for schema, name, market_id in (
            (
                "polymarket_wc2026_marts",
                "polymarket_wc2026_markets",
                "wc-market",
            ),
            (
                "polymarket_us_midterms_2026_marts",
                "polymarket_us_midterms_2026_markets",
                "mid-market",
            ),
        ):
            conn.execute(
                f"""
                create table {schema}.{name} (
                    event_id varchar,
                    event_slug varchar,
                    market_id varchar,
                    question varchar,
                    description varchar,
                    outcomes varchar,
                    clob_token_ids varchar,
                    start_time timestamp,
                    end_time timestamp,
                    category varchar,
                    tags varchar
                )
                """
            )
            conn.execute(
                f"""
                insert into {schema}.{name} values (
                    'evt', 'slug', ?, 'Question?', 'Rules',
                    '["Yes","No"]', '["y","n"]',
                    timestamp '2026-06-01 00:00:00',
                    timestamp '2026-07-01 00:00:00',
                    'Sports', '[]'
                )
                """,
                [market_id],
            )

        assert mart_exists(conn, "polymarket_wc2026_marts", "polymarket_wc2026_markets")
        out = tmp_path / "one.parquet"
        assert (
            export_one(
                conn,
                "polymarket_wc2026_marts",
                "polymarket_wc2026_markets",
                out,
            )
            == 1
        )
        row = conn.execute(
            "select market_id, question from read_parquet(?)", [str(out)]
        ).fetchone()
        assert row == ("wc-market", "Question?")

        results = export_all(conn, tmp_path, timestamp="20260101T000000Z")
        assert len(results) == 2
        assert all(count == 1 for _, _, count in results)
        assert {path.name for _, path, _ in results} == {
            "polymarket_wc2026_markets_20260101T000000Z.parquet",
            "polymarket_us_midterms_2026_markets_20260101T000000Z.parquet",
        }

        conn.execute(
            "drop table polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_markets"
        )
        try:
            export_all(conn, tmp_path, timestamp="20260101T000001Z")
            raise AssertionError("expected LookupError for missing midterms mart")
        except LookupError as exc:
            assert "polymarket_us_midterms_2026_markets" in str(exc)
            assert "--scope" in str(exc)
    finally:
        conn.close()
