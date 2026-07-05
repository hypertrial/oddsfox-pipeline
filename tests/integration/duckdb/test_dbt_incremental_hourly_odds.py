"""Integration coverage for the incremental hourly odds dbt fact."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
from tests.integration.conftest import write_dbt_profile

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.storage.duckdb.connection import init_duck_db

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_ROOT = REPO_ROOT / "dbt"
FACT_RELATION = (
    '"polymarket_wc2026_intermediate"."int_polymarket_wc2026_token_hourly_odds"'
)
ODDS_HISTORY = '"polymarket_wc2026_raw"."odds_history"'


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(DBT_ROOT),
        "--profiles-dir",
        str(profiles_dir),
    ]
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def _fact_rows(db_path: Path) -> list[tuple]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        return conn.execute(
            f"""
            select
                clob_token_id,
                odds_hour_epoch,
                open_price,
                high_price,
                low_price,
                close_price,
                avg_price,
                observed_points,
                first_timestamp,
                last_timestamp,
                latest_ingested_at
            from {FACT_RELATION}
            order by clob_token_id, odds_hour_epoch
            """
        ).fetchall()


def _insert_odds_rows(db_path: Path, rows: list[tuple]) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.executemany(
            f"""
            insert or replace into {ODDS_HISTORY}
            (clobTokenId, timestamp, price, ingested_at)
            values (?, ?, ?, ?)
            """,
            rows,
        )


def test_incremental_hourly_odds_reprocesses_late_dirty_hour(
    tmp_path: Path,
    monkeypatch,
    dbt_profiles_dir: Path,
) -> None:
    db_path = tmp_path / "hourly_incremental.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    write_dbt_profile(dbt_profiles_dir, db_path)
    connection.reset_duckdb_connection_state()
    init_duck_db()

    base_hour = datetime.now(timezone.utc).replace(
        minute=0,
        second=0,
        microsecond=0,
    ) - timedelta(days=1)
    initial_ingested_at = base_hour + timedelta(hours=1)
    late_ingested_at = base_hour + timedelta(hours=4)
    token_id = "token-a"
    initial_rows = [
        (
            token_id,
            int((base_hour + timedelta(minutes=5)).timestamp()),
            0.20,
            initial_ingested_at,
        ),
        (
            token_id,
            int((base_hour + timedelta(minutes=40)).timestamp()),
            0.60,
            initial_ingested_at,
        ),
        (
            token_id,
            int((base_hour + timedelta(hours=1, minutes=10)).timestamp()),
            0.30,
            initial_ingested_at,
        ),
    ]
    _insert_odds_rows(db_path, initial_rows)

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    env["DBT_PROFILES_DIR"] = str(dbt_profiles_dir)
    _run_dbt(
        ["seed", "--select", "polymarket_wc2026_contract"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    _run_dbt(
        [
            "run",
            "--full-refresh",
            "--select",
            "+int_polymarket_wc2026_token_hourly_odds",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    late_row = (
        token_id,
        int((base_hour + timedelta(minutes=50)).timestamp()),
        0.90,
        late_ingested_at,
    )
    _insert_odds_rows(db_path, [late_row])
    _run_dbt(
        ["run", "--select", "int_polymarket_wc2026_token_hourly_odds"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    incremental_rows = _fact_rows(db_path)
    assert incremental_rows[0][5] == 0.90
    assert incremental_rows[0][7] == 3

    _run_dbt(
        [
            "run",
            "--full-refresh",
            "--select",
            "+int_polymarket_wc2026_token_hourly_odds",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    assert incremental_rows == _fact_rows(db_path)
