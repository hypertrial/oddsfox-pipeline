"""Tests for scripts/export_eloratings_wc2026_team_ratings_freezes.py."""

from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import export_eloratings_wc2026_team_ratings_freezes as mod

    return mod


def _seed_marts(conn: duckdb.DuckDBPyConnection) -> None:
    collected = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    conn.execute("create schema wc2026_marts")
    conn.execute(
        """
        create table wc2026_marts.team_ratings_current (
            rank integer,
            team_code varchar,
            team_name varchar,
            rating double,
            snapshot_id varchar,
            collected_at timestamptz
        )
        """
    )
    conn.execute(
        """
        create table wc2026_marts.team_ratings_history (
            snapshot_year integer,
            snapshot_scope varchar,
            rank integer,
            team_code varchar,
            team_name varchar,
            rating double,
            snapshot_id varchar,
            collected_at timestamptz
        )
        """
    )
    conn.execute(
        """
        insert into wc2026_marts.team_ratings_current values
            (1, 'ES', 'Spain', 2100.0, 'snap-1', ?),
            (2, 'AR', 'Argentina', 2050.0, 'snap-1', ?)
        """,
        [collected, collected],
    )
    conn.execute(
        """
        insert into wc2026_marts.team_ratings_history values
            (2025, '2025', 1, 'ES', 'Spain', 2080.0, 'snap-1', ?),
            (2025, '2025', 2, 'AR', 'Argentina', 2040.0, 'snap-1', ?),
            (2024, '2024', 1, 'AR', 'Argentina', 2000.0, 'snap-1', ?),
            (NULL, 'current', 1, 'ES', 'Spain', 2100.0, 'snap-1', ?)
        """,
        [collected, collected, collected, collected],
    )


def test_export_eloratings_wc2026_team_ratings_freezes_writes_both_csvs(
    tmp_path: Path,
) -> None:
    mod = _load_export_module()
    conn = duckdb.connect()
    try:
        _seed_marts(conn)
        counts = mod.export_eloratings_wc2026_team_ratings_freezes(conn, tmp_path)
    finally:
        conn.close()

    assert counts == {"pre_kickoff": 2, "latest_current": 2}

    pre_path = tmp_path / mod.PRE_KICKOFF_FILE
    latest_path = tmp_path / mod.LATEST_CURRENT_FILE
    assert pre_path.is_file()
    assert latest_path.is_file()

    with pre_path.open(encoding="utf-8", newline="") as handle:
        pre_rows = list(csv.DictReader(handle))
    with latest_path.open(encoding="utf-8", newline="") as handle:
        latest_rows = list(csv.DictReader(handle))

    assert [row["team_code"] for row in pre_rows] == ["ES", "AR"]
    assert {row["team_code"] for row in pre_rows} == {"ES", "AR"}
    assert all(row["freeze_label"] == "pre_kickoff" for row in pre_rows)
    assert all(row["as_of"] == "2025-12-31" for row in pre_rows)
    assert pre_rows[0]["rating"] == "2080.0"

    assert [row["team_code"] for row in latest_rows] == ["ES", "AR"]
    assert {row["team_code"] for row in latest_rows} == {"ES", "AR"}
    assert all(row["freeze_label"] == "latest_current" for row in latest_rows)
    assert latest_rows[0]["rating"] == "2100.0"


def test_export_eloratings_wc2026_team_ratings_freezes_requires_pre_kickoff_year(
    tmp_path: Path,
) -> None:
    mod = _load_export_module()
    conn = duckdb.connect()
    try:
        conn.execute("create schema wc2026_marts")
        conn.execute(
            """
            create table wc2026_marts.team_ratings_current (
                rank integer,
                team_code varchar,
                team_name varchar,
                rating double,
                snapshot_id varchar,
                collected_at timestamptz
            )
            """
        )
        conn.execute(
            """
            create table wc2026_marts.team_ratings_history (
                snapshot_year integer,
                snapshot_scope varchar,
                rank integer,
                team_code varchar,
                team_name varchar,
                rating double,
                snapshot_id varchar,
                collected_at timestamptz
            )
            """
        )
        conn.execute(
            """
            insert into wc2026_marts.team_ratings_current
            values (1, 'ES', 'Spain', 2100.0, 'snap-1', ?)
            """,
            [datetime(2026, 7, 25, tzinfo=timezone.utc)],
        )
        with pytest.raises(LookupError, match="snapshot_year=2025"):
            mod.export_eloratings_wc2026_team_ratings_freezes(conn, tmp_path)
    finally:
        conn.close()
