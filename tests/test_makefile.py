"""Makefile recipe sanity checks."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _dagster_dev_shell_script() -> str:
    proc = subprocess.run(
        ["make", "-n", "dagster-dev"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    lines = proc.stdout.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("cd "))
    script_lines: list[str] = []
    for line in lines[start:]:
        stripped = line.rstrip()
        if stripped.endswith("\\"):
            script_lines.append(stripped[:-1].strip())
            continue
        script_lines.append(stripped.strip())
        break
    script = " ".join(script_lines)
    assert script.startswith("cd "), proc.stdout
    return script


def _noop_dev_command(script: str, replacement: str) -> str:
    script = re.sub(r'"[^"]+/dg" dev\b.*?(?=; else |; fi)', replacement, script)
    return re.sub(
        r'"[^"]+/python[^"]*" -m dagster dev\b.*?(?=; fi)',
        replacement,
        script,
    )


def test_dagster_dev_recipe_is_valid_posix_sh():
    script = _noop_dev_command(_dagster_dev_shell_script(), "true")
    subprocess.run(["/bin/sh", "-n", "-c", script], check=True)


def test_dagster_dev_recipe_prefers_dg_with_python_fallback():
    script = _dagster_dev_shell_script()
    assert 'if test -x "' in script
    assert '/.venv/bin/dg" dev' in script
    assert "-m dagster dev" in script


def test_ci_split_targets_remain_wired():
    makefile = (REPO_ROOT / "Makefile").read_text()

    assert "gx-data-quality:" in makefile
    assert "data-quality: dbt-build-ci gx-data-quality" in makefile
    assert "costguard-scan:" in makefile
    assert "costguard: dbt-build-ci costguard-scan" in makefile
    assert "dagster-jobs-smoke-cov:" in makefile
    assert "dagster-refresh-cov:" in makefile
    assert "integration-dagster-cov: dagster-jobs-smoke-cov dagster-refresh-cov" in (
        makefile
    )
    assert "live-smoke:" in makefile
    assert "wc2026_knockout_match_odds_full_pipeline" in makefile
    assert "-c config/live-smoke.yaml" in makefile
    assert "match-minute-live-smoke:" in makefile
    assert "polymarket_wc2026_match_minute_odds_backfill" in makefile
    assert 'cd "$(REPO_ROOT)/.cache"' in makefile
    assert '-d "$(REPO_ROOT)"' in makefile
    assert "latest_fetch_hash_issues, elapsed_axis_issue_markets" in makefile
    assert "'published', 496, 496, 0, 0, 0, 496, 0, 0, 0, None" in makefile

    live_smoke_config = (REPO_ROOT / "config" / "live-smoke.yaml").read_text()
    assert "polymarket_wc2026_raw_token_odds_history_hourly:" in live_smoke_config
    assert "kalshi_wc2026_raw_market_candlesticks_hourly:" in live_smoke_config
    assert live_smoke_config.count("window_hours: 24") == 2
    assert live_smoke_config.count("history_backfill_days: 0") == 2
    assert "min_volume: 5000.0" in live_smoke_config


def test_polygon_settlement_live_smoke_is_fail_closed_to_disposable_database():
    proc = subprocess.run(
        ["make", "-n", "polygon-settlement-live-smoke"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    recipe = proc.stdout
    runtime = REPO_ROOT / ".cache" / "polygon_settlement"
    expected = runtime / "benchmarks" / "v4" / "live_smoke.duckdb"

    assert f'cd "{runtime}" &&' in recipe
    assert f'DUCKDB_NAME="{expected}"' in recipe
    assert f'DUCKDB_PATH="{expected}"' in recipe
    assert f'TMPDIR="{runtime}/tmp"' in recipe
    assert f'XDG_CACHE_HOME="{runtime}/xdg"' in recipe
    assert f'UV_CACHE_DIR="{REPO_ROOT}/.cache/uv"' in recipe
    assert f'DUCKDB_EXTENSION_DIRECTORY="{runtime}/duckdb-extensions"' in recipe
    assert f'DAGSTER_HOME="{runtime}/dagster"' in recipe
    assert f'DBT_TARGET_PATH="{runtime}/dbt-target"' in recipe
    assert f'DBT_LOG_PATH="{runtime}/dbt-logs"' in recipe
    assert 'test "false" = "true"' in recipe
    assert "execute_in_process" in recipe
    assert "assert_disposable_duckdb_path(expected)" in recipe
    assert "config = run_config(expected_duckdb_path=expected" in recipe
    assert 'POLYGON_SETTLEMENT_LIVE_SMOKE_REQUESTS_PER_SECOND="5"' in recipe
    assert 'POLYGON_SETTLEMENT_LIVE_SMOKE_WORKERS="5"' in recipe
    assert 'POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_BLOCK_CHUNK_SIZE="8000"' in recipe
    assert 'POLYGON_SETTLEMENT_LIVE_SMOKE_INITIAL_RECEIPT_BATCH_SIZE="20"' in recipe


def test_polygon_settlement_export_is_offline_and_reads_the_audit_release():
    proc = subprocess.run(
        [
            "make",
            "-n",
            "POLYGON_DATASET_VERSION=1.2.3",
            "polygon-settlement-export",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    recipe = proc.stdout
    assert "export_polymarket_wc2026_polygon_settlement_minute_odds.py" in recipe
    assert (
        '--audit-release "artifacts/polygon_settlement/audit/releases/1.2.3"' in recipe
    )
    assert '--output-root "artifacts/polygon_settlement/exports"' in recipe
    assert "polygon-runtime-dirs" not in recipe
