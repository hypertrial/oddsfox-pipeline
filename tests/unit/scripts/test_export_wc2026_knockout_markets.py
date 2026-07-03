"""Tests for scripts/export_wc2026_knockout_markets.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_wc2026_knockout_markets import (
        export_wc2026_knockout_markets,
        mart_exists,
    )

    return export_wc2026_knockout_markets, mart_exists


def test_export_wc2026_knockout_markets_round_trip(tmp_path: Path) -> None:
    export_wc2026_knockout_markets, mart_exists = _load_export_module()
    out_path = tmp_path / "wc2026_knockout_markets.parquet"
    conn = duckdb.connect()
    try:
        conn.execute("create schema polymarket_marts")
        conn.execute(
            """
            create table polymarket_marts.wc2026_knockout_markets (
                market_id varchar,
                clob_token_id varchar,
                stage_key varchar,
                team_name varchar
            )
            """
        )
        conn.execute(
            """
            insert into polymarket_marts.wc2026_knockout_markets
            values ('m1', 'tok-a', 'winner', 'Alpha')
            """
        )
        assert mart_exists(conn) is True
        assert export_wc2026_knockout_markets(conn, out_path) == 1
        got = conn.execute("select * from read_parquet(?)", [str(out_path)]).fetchall()
    finally:
        conn.close()

    assert got == [("m1", "tok-a", "winner", "Alpha")]
