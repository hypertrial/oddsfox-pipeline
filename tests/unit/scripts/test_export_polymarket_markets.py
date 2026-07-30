"""Tests for scripts/export_polymarket_markets.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_polymarket_markets import (
        export_all_polymarket_markets_catalogs,
        export_polymarket_markets_catalog,
        mart_exists,
        validate_catalog_export,
    )

    return (
        export_polymarket_markets_catalog,
        export_all_polymarket_markets_catalogs,
        mart_exists,
        validate_catalog_export,
    )


def _create_catalog_mart(
    conn: duckdb.DuckDBPyConnection, schema: str, name: str, market_id: str
) -> None:
    conn.execute(f"create schema if not exists {schema}")
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
            volume double,
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
            '["Yes","No"]', '["y","n"]', 150000.0,
            timestamp '2026-06-01 00:00:00',
            timestamp '2026-07-01 00:00:00',
            'Sports', '[]'
        )
        """,
        [market_id],
    )


def test_export_polymarket_markets_catalog_round_trip(tmp_path: Path) -> None:
    export_one, export_all, mart_exists, validate = _load_export_module()
    conn = duckdb.connect()
    try:
        _create_catalog_mart(
            conn, "polymarket_wc2026_marts", "polymarket_wc2026_markets", "wc-market"
        )
        _create_catalog_mart(
            conn,
            "polymarket_us_midterms_2026_marts",
            "polymarket_us_midterms_2026_markets",
            "mid-market",
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
            "select market_id, question, volume from read_parquet(?)", [str(out)]
        ).fetchone()
        assert row == ("wc-market", "Question?", 150000.0)
        assert validate(conn, out)["row_count"] == 1

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


def test_validate_catalog_export_rejects_below_volume_floor(tmp_path: Path) -> None:
    _, _, _, validate = _load_export_module()
    conn = duckdb.connect()
    try:
        path = tmp_path / "bad.parquet"
        conn.execute(
            """
            copy (
              select
                'evt' as event_id, 'slug' as event_slug, 'm1' as market_id,
                'Q' as question, '' as description, '["Yes","No"]' as outcomes,
                '["y","n"]' as clob_token_ids, 99999.0 as volume,
                cast(null as timestamp) as start_time,
                cast(null as timestamp) as end_time,
                cast(null as varchar) as category,
                cast(null as varchar) as tags
            ) to ?
            (format parquet)
            """,
            [str(path)],
        )
        with pytest.raises(ValueError, match="below"):
            validate(conn, path)
    finally:
        conn.close()


def test_export_rejects_start_after_end_without_writing_final(
    tmp_path: Path,
) -> None:
    export_one, _, _, _ = _load_export_module()
    conn = duckdb.connect()
    try:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.polymarket_wc2026_markets as
            select
              'evt' as event_id, 'slug' as event_slug, 'm1' as market_id,
              'Q' as question, '' as description, '["Yes","No"]' as outcomes,
              '["y","n"]' as clob_token_ids, 150000.0 as volume,
              timestamp '2026-07-02 00:00:00' as start_time,
              timestamp '2026-07-01 00:00:00' as end_time,
              cast(null as varchar) as category,
              cast(null as varchar) as tags
            """
        )
        out = tmp_path / "catalog.parquet"
        with pytest.raises(ValueError, match="start_time > end_time"):
            export_one(
                conn,
                "polymarket_wc2026_marts",
                "polymarket_wc2026_markets",
                out,
            )
        assert not out.exists()
        assert not out.with_suffix(out.suffix + ".tmp").exists()
    finally:
        conn.close()
