"""Integration coverage for repeated canonical snapshot consumption."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
from tests.integration.conftest import write_dbt_profile

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_ROOT = REPO_ROOT / "dbt"


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            *args,
            "--project-dir",
            str(DBT_ROOT),
            "--profiles-dir",
            str(profiles_dir),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def _insert_ledger_rows(conn: duckdb.DuckDBPyConnection) -> None:
    conn.executemany(
        """
        insert into wc2026_ops.raw_snapshot_ledger (
            source, snapshot_id, collected_at, collector_git_sha,
            collector_container_digest, manifest_sha256
        ) values (?, ?, cast(? as timestamptz), ?, ?, ?)
        """,
        [
            (source, snapshot_id, collected_at, "git", "image", "manifest")
            for source, snapshot_id, collected_at in [
                ("eloratings", "elo-old", "2026-07-17T00:00:00Z"),
                ("eloratings", "elo-new", "2026-07-18T00:00:00Z"),
                ("clubelo", "club-old", "2026-07-17T00:00:00Z"),
                ("clubelo", "club-new", "2026-07-18T00:00:00Z"),
                ("fifaindex", "fifa-old", "2026-07-17T00:00:00Z"),
                ("fifaindex", "fifa-new", "2026-07-18T00:00:00Z"),
                (
                    "wikipedia_squads",
                    "squad-old",
                    "2026-07-17T00:00:00Z",
                ),
                (
                    "wikipedia_squads",
                    "squad-new",
                    "2026-07-18T00:00:00Z",
                ),
                ("fotmob", "events-old", "2026-07-17T00:00:00Z"),
                ("fotmob", "events-new", "2026-07-18T00:00:00Z"),
            ]
        ],
    )


def _insert_canonical_rows(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        insert into wc2026_raw.eloratings__team_ratings values
        (1, 'USA', 'United States', 1800, 2025, 'year_end',
         'eloratings', 'elo-old', '2026-07-17T00:00:00Z'),
        (2, 'CAN', 'Canada', 1750, 2025, 'year_end',
         'eloratings', 'elo-old', '2026-07-17T00:00:00Z'),
        (1, 'USA', 'United States', 1810, 2025, 'year_end',
         'eloratings', 'elo-new', '2026-07-18T00:00:00Z')
        """
    )
    conn.execute(
        """
        insert into wc2026_raw.clubelo__club_ratings values
        ('2026-07-17', 'club-a', 'Club A', 'Club A', 'USA', 1500, 1,
         '2026-01-01', null, 'clubelo', 'club-old', '2026-07-17T00:00:00Z'),
        ('2026-07-17', 'club-b', 'Club B', 'Club B', 'CAN', 1400, 2,
         '2026-01-01', null, 'clubelo', 'club-old', '2026-07-17T00:00:00Z'),
        ('2026-07-18', 'club-a', 'Club A', 'Club A', 'USA', 1510, 1,
         '2026-01-01', null, 'clubelo', 'club-new', '2026-07-18T00:00:00Z')
        """
    )
    conn.execute(
        """
        insert into wc2026_raw.fifaindex__players (
            game_slug, competition_key, player_id, player_name, nationality,
            overall, player_gender, was_world_cup_squad_member,
            _source, _snapshot_id, _collected_at
        ) values
        ('fc26', 'world-cup', 1, 'Player One', 'United States', 70,
         'male', true, 'fifaindex', 'fifa-old', '2026-07-17T00:00:00Z'),
        ('fc26', 'world-cup', 2, 'Removed Player', 'Canada', 75,
         'male', true, 'fifaindex', 'fifa-old', '2026-07-17T00:00:00Z'),
        ('fc26', 'world-cup', 1, 'Player One', 'United States', 80,
         'male', true, 'fifaindex', 'fifa-new', '2026-07-18T00:00:00Z')
        """
    )
    conn.execute(
        """
        insert into wc2026_raw.wikipedia_squads__players (
            source_player_key, run_id, official_wc2026_squad_team,
            source_team_code, official_wc2026_player_name,
            official_wc2026_squad_position, official_wc2026_squad_caps,
            _source, _snapshot_id, _collected_at
        ) values
        ('player-one', 'old-run', 'United States', 'USA', 'Player One',
         'MID', 10, 'wikipedia_squads', 'squad-old', '2026-07-17T00:00:00Z'),
        ('removed-player', 'old-run', 'Canada', 'CAN', 'Removed Player',
         'DEF', 5, 'wikipedia_squads', 'squad-old', '2026-07-17T00:00:00Z'),
        ('player-one', 'new-run', 'United States', 'USA', 'Player One',
         'MID', 20, 'wikipedia_squads', 'squad-new', '2026-07-18T00:00:00Z')
        """
    )
    conn.execute(
        """
        insert into wc2026_raw.fotmob__events (
            match_id, event_id, event_type, _source, _snapshot_id, _collected_at
        ) values
        ('match-1', 'old-event', 'goal', 'fotmob', 'events-old',
         '2026-07-17T00:00:00Z'),
        ('match-1', 'new-event', 'goal', 'fotmob', 'events-new',
         '2026-07-18T00:00:00Z')
        """
    )


def test_strategy_marts_use_only_latest_complete_canonical_snapshots(
    tmp_path: Path,
    dbt_profiles_dir: Path,
) -> None:
    db_path = tmp_path / "canonical_snapshots.duckdb"
    write_dbt_profile(dbt_profiles_dir, db_path)
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    env["DBT_PROFILES_DIR"] = str(dbt_profiles_dir)

    _run_dbt(
        ["seed", "--select", "wc2026_team_canonical_aliases"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path)) as conn:
        _insert_ledger_rows(conn)
        _insert_canonical_rows(conn)
        with pytest.raises(duckdb.ConstraintException):
            conn.execute(
                """
                insert into wc2026_ops.raw_snapshot_ledger values
                ('eloratings', 'elo-old', current_timestamp, 'git', 'image',
                 'manifest', current_timestamp)
                """
            )

    _run_dbt(
        [
            "run",
            "--select",
            "wc2026_team_ratings_current",
            "wc2026_team_ratings_history",
            "wc2026_club_strength_current",
            "wc2026_club_strength_history",
            "wc2026_club_strength_snapshot",
            "wc2026_player_features",
            "wc2026_squad_player_features",
            "wc2026_event_state_timing",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            """
            select team_code, rating, snapshot_id
            from wc2026_marts.team_ratings_history
            """
        ).fetchall() == [("USA", 1810.0, "elo-new")]
        assert conn.execute(
            """
            select club_key, elo, snapshot_id
            from wc2026_marts.club_strength_snapshot
            """
        ).fetchall() == [("club-a", 1510.0, "club-new")]
        assert conn.execute(
            """
            select player_id, overall, official_wc2026_squad_caps, snapshot_id
            from wc2026_marts.player_features
            """
        ).fetchall() == [(1, 80.0, 20, "fifa-new")]
        assert conn.execute(
            """
            select source_player_key, overall, official_wc2026_squad_caps,
                   squad_snapshot_id, player_snapshot_id
            from wc2026_marts.squad_player_features
            """
        ).fetchall() == [("player-one", 80.0, 20, "squad-new", "fifa-new")]
        assert conn.execute(
            """
            select event_id, snapshot_id
            from wc2026_marts.event_state_timing
            """
        ).fetchall() == [("new-event", "events-new")]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            insert into wc2026_ops.raw_snapshot_ledger (
                source, snapshot_id, collected_at, collector_git_sha,
                collector_container_digest, manifest_sha256
            ) values (
                'eloratings', 'elo-empty', current_timestamp,
                'git', 'image', 'manifest'
            )
            """
        )
        conn.execute("create schema if not exists wc2026_marts")
        conn.execute(
            """
            create table wc2026_marts.price_liquidity_current as
            select current_timestamp as latest_point_odds_timestamp
            """
        )
        conn.execute(
            """
            create table wc2026_marts.fixtures as
            select value as match_id from range(104) as rows(value)
            """
        )
        conn.execute("create schema international_results_wc2026_raw")
        conn.execute(
            """
            create table international_results_wc2026_raw.historical_matches as
            select current_timestamp as source_loaded_at
            """
        )

    _run_dbt(
        ["run", "--select", "wc2026_source_availability"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            """
            select latest_snapshot_id, row_count, available
            from wc2026_observability.wc2026_source_availability
            where source = 'eloratings'
            """
        ).fetchone() == ("elo-empty", 0, False)
