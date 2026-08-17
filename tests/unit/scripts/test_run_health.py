from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "run_health.py"


def _health_db(path: Path, *, warning_count: int, critical_count: int) -> None:
    status = "critical" if critical_count else "warning" if warning_count else "healthy"
    with duckdb.connect(str(path)) as conn:
        conn.execute("create schema polymarket_soccer_observability")
        conn.execute(
            """
            create table polymarket_soccer_observability.polymarket_soccer_pipeline_health as
            select 'run-1' as dagster_run_id, 'success' as latest_run_status,
                current_timestamp as latest_run_started_at,
                current_timestamp as latest_run_finished_at,
                ?::bigint as warning_count, ?::bigint as critical_count,
                ? as health_status, current_timestamp as measured_at
            """,
            [warning_count, critical_count, status],
        )


@pytest.mark.parametrize(
    ("warnings", "criticals", "expected_exit"),
    [(0, 0, 0), (1, 0, 0), (0, 1, 1)],
)
def test_soccer_health_json_and_exit_contract(
    tmp_path: Path, warnings: int, criticals: int, expected_exit: int
) -> None:
    database = tmp_path / "health.duckdb"
    _health_db(database, warning_count=warnings, critical_count=criticals)
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--scope",
            "polymarket:soccer",
            "--fail-on",
            "critical",
            "--format",
            "json",
            "--duckdb-path",
            str(database),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == expected_exit
    assert json.loads(result.stdout)["health_status"] == (
        "critical" if criticals else "warning" if warnings else "healthy"
    )


def test_soccer_health_unreadable_state_returns_two(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--scope",
            "polymarket:soccer",
            "--duckdb-path",
            str(tmp_path / "missing.duckdb"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "unavailable" in result.stdout


def test_soccer_health_invalid_state_returns_two(tmp_path: Path) -> None:
    database = tmp_path / "invalid.duckdb"
    _health_db(database, warning_count=1, critical_count=0)
    with duckdb.connect(str(database)) as conn:
        conn.execute(
            """
            update polymarket_soccer_observability.polymarket_soccer_pipeline_health
            set health_status = 'healthy'
            """
        )
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--scope",
            "polymarket:soccer",
            "--format",
            "json",
            "--duckdb-path",
            str(database),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["status"] == "unavailable"
