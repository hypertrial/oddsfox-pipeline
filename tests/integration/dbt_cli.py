"""Shared dbt subprocess helpers for integration tests."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_ROOT = REPO_ROOT / "dbt"


def isolated_dbt_env(
    *,
    db_path: Path,
    profiles_dir: Path,
    target_dir: Path,
    log_dir: Path | None = None,
    base_env: dict[str, str] | None = None,
    dbt_threads: int = 1,
) -> dict[str, str]:
    """Build an env with per-test DuckDB + dbt artifact isolation."""
    env = (base_env or os.environ).copy()
    target_dir.mkdir(parents=True, exist_ok=True)
    resolved_log_dir = log_dir or (target_dir.parent / "dbt-logs")
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    env["DBT_TARGET_PATH"] = str(target_dir)
    env["DBT_LOG_PATH"] = str(resolved_log_dir)
    env["DBT_THREADS"] = str(dbt_threads)
    return env


def run_dbt(
    args: list[str],
    *,
    profiles_dir: Path,
    env: dict[str, str],
    project_dir: Path | None = None,
    expect_fail: bool = False,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run dbt via the active interpreter; assert success unless expect_fail."""
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(project_dir or DBT_ROOT),
        "--profiles-dir",
        str(profiles_dir),
    ]
    completed = subprocess.run(
        command,
        cwd=str(cwd or REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    if expect_fail:
        assert completed.returncode != 0, completed.stdout + completed.stderr
    else:
        assert completed.returncode == 0, completed.stdout + completed.stderr
    return completed
