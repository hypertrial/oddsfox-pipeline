"""Tests for scripts/export_polymarket_wc2026_knockout_hourly_odds.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_polymarket_wc2026_knockout_hourly_odds import (
        export_polymarket_wc2026_knockout_hourly_odds,
        mart_exists,
    )

    return export_polymarket_wc2026_knockout_hourly_odds, mart_exists


def test_export_polymarket_wc2026_knockout_hourly_odds_round_trip(
    tmp_path: Path,
) -> None:
    export_polymarket_wc2026_knockout_hourly_odds, mart_exists = _load_export_module()
    out_path = tmp_path / "polymarket_wc2026_knockout_token_hourly_odds.parquet"
    conn = duckdb.connect()
    try:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds (
                market_id varchar,
                clob_token_id varchar,
                stage_key varchar,
                market_direction varchar,
                team_name varchar,
                odds_hour_epoch bigint,
                close_price double
            )
            """
        )
        conn.execute(
            """
            insert into polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds
            values ('m1', 'tok-a', 'winner', 'winner', 'Alpha', 1782604800, 0.42)
            """
        )
        assert mart_exists(conn) is True
        assert export_polymarket_wc2026_knockout_hourly_odds(conn, out_path) == 1
        got = conn.execute("select * from read_parquet(?)", [str(out_path)]).fetchall()
    finally:
        conn.close()

    assert got == [("m1", "tok-a", "winner", "winner", "Alpha", 1782604800, 0.42)]


def test_export_polymarket_wc2026_knockout_hourly_odds_filters(
    tmp_path: Path,
) -> None:
    export_polymarket_wc2026_knockout_hourly_odds, _mart_exists = _load_export_module()
    conn = duckdb.connect()
    try:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds (
                market_id varchar,
                clob_token_id varchar,
                is_live_market boolean,
                is_still_alive boolean,
                close_price double
            )
            """
        )
        conn.execute(
            """
            insert into polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds
            values
                ('m-live-active', 'tok-live-active', true, true, 0.7),
                ('m-closed-active', 'tok-closed-active', false, true, 0.9),
                ('m-live-eliminated', 'tok-live-eliminated', true, false, 0.2)
            """
        )

        default_path = tmp_path / "default.parquet"
        live_path = tmp_path / "live.parquet"
        active_path = tmp_path / "active.parquet"
        combined_path = tmp_path / "combined.parquet"

        assert export_polymarket_wc2026_knockout_hourly_odds(conn, default_path) == 3
        assert (
            export_polymarket_wc2026_knockout_hourly_odds(
                conn, live_path, live_only=True
            )
            == 2
        )
        assert (
            export_polymarket_wc2026_knockout_hourly_odds(
                conn, active_path, active_teams_only=True
            )
            == 2
        )
        assert (
            export_polymarket_wc2026_knockout_hourly_odds(
                conn,
                combined_path,
                live_only=True,
                active_teams_only=True,
            )
            == 1
        )

        combined_tokens = conn.execute(
            "select clob_token_id from read_parquet(?)",
            [str(combined_path)],
        ).fetchall()
    finally:
        conn.close()

    assert combined_tokens == [("tok-live-active",)]
