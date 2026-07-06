"""Tests for scripts/export_polymarket_wc2026_graph_hourly_odds.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_polymarket_wc2026_graph_hourly_odds import (
        export_polymarket_wc2026_graph_hourly_odds,
        mart_exists,
    )

    return export_polymarket_wc2026_graph_hourly_odds, mart_exists


def test_export_polymarket_wc2026_graph_hourly_odds_contract(tmp_path: Path) -> None:
    export_graph_hourly, mart_exists = _load_export_module()
    out_path = tmp_path / "polymarket_wc2026_graph_token_hourly_odds.parquet"
    conn = duckdb.connect()
    try:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds (
                market_id varchar,
                outcome_index integer,
                clob_token_id varchar,
                question varchar,
                outcome_label varchar,
                event_slug varchar,
                is_active boolean,
                is_closed boolean,
                market_volume_usd double,
                odds_hour_utc timestamp,
                odds_hour_epoch bigint,
                close_price double,
                stage_key varchar,
                stage_rank integer,
                canonical_team_name varchar,
                market_direction varchar,
                progression_outcome_label varchar,
                is_progression_token boolean,
                opposite_clob_token_id varchar,
                market_status varchar,
                is_still_alive boolean
            )
            """
        )
        conn.execute(
            """
            insert into polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds
            values
                ('m1', 0, 'tok-yes', 'Will Alpha win?', 'Yes', 'event', true, false, 10000, timestamp '2026-07-01 00:00:00', 1782864000, 0.4, 'winner', 5, 'Alpha', 'winner', 'win_world_cup', true, 'tok-no', 'live', true),
                ('m1', 1, 'tok-no', 'Will Alpha win?', 'No', 'event', true, false, 10000, timestamp '2026-07-01 00:00:00', 1782864000, 0.6, 'winner', 5, 'Alpha', 'winner', 'win_world_cup', false, 'tok-yes', 'live', true)
            """
        )

        assert mart_exists(conn) is True
        assert export_graph_hourly(conn, out_path) == 2
        rows = conn.execute(
            "select outcome_label, is_progression_token from read_parquet(?) order by outcome_index",
            [str(out_path)],
        ).fetchall()
        cols = [
            row[0]
            for row in conn.execute(
                "describe select * from read_parquet(?)", [str(out_path)]
            ).fetchall()
        ]
    finally:
        conn.close()

    assert rows == [("Yes", True), ("No", False)]
    assert {
        "outcome_label",
        "stage_key",
        "canonical_team_name",
        "is_progression_token",
        "opposite_clob_token_id",
    }.issubset(cols)
