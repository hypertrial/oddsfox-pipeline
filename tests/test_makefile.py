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

    live_smoke_config = (REPO_ROOT / "config" / "live-smoke.yaml").read_text()
    assert "polymarket_wc2026_raw_token_odds_history_hourly:" in live_smoke_config
    assert "kalshi_wc2026_raw_market_candlesticks_hourly:" in live_smoke_config
    assert live_smoke_config.count("window_hours: 24") == 2
    assert live_smoke_config.count("history_backfill_days: 0") == 2
    assert "min_volume: 5000.0" in live_smoke_config
