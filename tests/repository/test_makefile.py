"""Makefile recipe sanity checks."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from tests.support.makefile_text import makefile_text

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE_FRAGMENTS = (
    "Makefile.gates",
    "Makefile.dbt",
    "Makefile.lint",
    "Makefile.test",
    "Makefile.ops",
)


def _target_recipe(makefile: str, target: str) -> str:
    match = re.search(
        rf"^{re.escape(target)}(?:\s*:[^\n]*)?\n(?P<recipe>(?:\t[^\n]*\n)+)",
        makefile,
        re.MULTILINE,
    )
    assert match, target
    return match.group("recipe")


def _recursive_make_targets(recipe: str) -> list[str]:
    prefix = "$(MAKE) "
    targets: list[str] = []
    for line in recipe.splitlines():
        stripped = line.strip()
        if not stripped.startswith(prefix):
            continue
        tokens = stripped.removeprefix(prefix).split()
        # Skip make flags such as -j3 / ODDSFOX_RUNTIME_ROOT=... assignments.
        goal_tokens = [
            token for token in tokens if not token.startswith("-") and "=" not in token
        ]
        targets.extend(goal_tokens)
    return targets


def _target_prerequisites(makefile: str, target: str) -> list[str]:
    match = re.search(
        rf"^{re.escape(target)}\s*:(?P<deps>[^\n]*)\n",
        makefile,
        re.MULTILINE,
    )
    assert match, target
    return [token for token in match.group("deps").split() if token]


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


def test_dagster_dev_uses_short_socket_temp_directory():
    script = _dagster_dev_shell_script()
    expected = f"/tmp/oddsfox-dg-{os.getuid()}"
    recipe = _target_recipe(makefile_text(), "dagster-dev")

    assert f'export TMPDIR="{expected}" TMP="{expected}" TEMP="{expected}"' in script
    assert 'chmod 700 "$(DAGSTER_DEV_TMP)"' in recipe
    assert len(expected) < 40


def test_makefile_include_fragments_are_present_and_inlined():
    root = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    for name in MAKEFILE_FRAGMENTS:
        assert f"include {name}" in root
        assert (REPO_ROOT / name).is_file()
    inlined = makefile_text()
    assert "include Makefile." not in inlined
    assert "ci-fast:" in inlined
    assert "test-dev:" in inlined


def test_ci_split_targets_remain_wired():
    makefile = makefile_text()

    assert "gx-data-quality" not in makefile
    assert re.search(r"^data-quality: dbt-build-ci$", makefile, re.MULTILINE)
    assert "mutmut run" in _target_recipe(makefile, "mutation")
    assert "mutmut export-cicd-stats" in _target_recipe(makefile, "mutation")
    assert "scripts/check_mutmut_stats.py" in _target_recipe(makefile, "mutation")
    assert 'rm -rf "$(REPO_ROOT)/mutants"' in _target_recipe(makefile, "mutation-ci")
    assert "mutants" in _target_recipe(makefile, "clean-local-artifacts")
    assert "mutants/" in (REPO_ROOT / ".gitignore").read_text().splitlines()
    assert _recursive_make_targets(_target_recipe(makefile, "mutation-ci")) == [
        "mutation"
    ]
    assert "costguard-scan:" in makefile
    assert "dbt-build-ci costguard-scan" in _target_recipe(makefile, "costguard")
    assert "-j1" in _target_recipe(makefile, "costguard")
    assert "dagster-jobs-smoke-cov:" in makefile
    assert "dagster-refresh-cov:" in makefile
    assert "integration-dagster-cov: dagster-jobs-smoke-cov dagster-refresh-cov" in (
        makefile
    )
    assert "match-minute-live-smoke:" in makefile
    assert "MATCH_MINUTE_LIVE_SMOKE_RUNTIME_ROOT" in makefile
    assert 'ODDSFOX_RUNTIME_ROOT="$(MATCH_MINUTE_LIVE_SMOKE_RUNTIME_ROOT)"' in makefile
    assert "minute-odds-live-smoke:" in makefile
    assert "polymarket_wc2026_minute_odds_live_smoke" in makefile
    assert "MINUTE_ODDS_LIVE_SMOKE_DUCKDB_PATH" in makefile
    assert "MINUTE_ODDS_LIVE_SMOKE_RESET" in makefile
    assert "MINUTE_ODDS_LIVE_SMOKE_REFRESH_CATALOG" in makefile
    assert "MINUTE_ODDS_LIVE_SMOKE_RUNTIME_ROOT" in makefile
    assert 'ODDSFOX_RUNTIME_ROOT="$(MINUTE_ODDS_LIVE_SMOKE_RUNTIME_ROOT)"' in makefile
    assert "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG=" in makefile
    assert "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_MATCH=true" in makefile
    assert "POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_FUTURES=true" in makefile
    assert "scripts/validate_polymarket_wc2026_minute_odds_live_smoke.py" in makefile
    assert "polymarket_wc2026_match_minute_odds_backfill" in makefile
    assert 'cd "$(REPO_ROOT)/.cache"' in makefile
    assert '-d "$(REPO_ROOT)"' in makefile
    assert "latest_fetch_hash_issues, elapsed_axis_issue_markets" in makefile
    assert "'published', 496, 496, 0, 0, 0, 496, 0, 0, 0, None" in makefile
    assert "match-minute-inputs-validate:" in makefile
    assert "futures-minute-publish-benchmark:" in makefile
    assert "scripts/benchmark_polymarket_wc2026_futures_minute_publish.py" in makefile
    assert "FUTURES_MINUTE_PUBLISH_BENCHMARK_ROOT" in makefile
    assert "minute-odds-dbt-benchmark:" in makefile
    assert "scripts/benchmark_polymarket_wc2026_minute_odds_dbt.py" in makefile
    assert "MINUTE_ODDS_DBT_BENCHMARK_ROOT" in makefile
    assert "local-marts-rebuild:" in makefile
    assert "+polymarket_wc2026_match_minute_odds" in makefile
    assert "+polymarket_wc2026_polygon_settlement_minute_odds" in makefile


def test_fast_and_coverage_tests_parallelize_only_the_safe_collection():
    makefile = makefile_text()
    fast = _target_recipe(makefile, "test")
    coverage = _target_recipe(makefile, "test-cov")

    for recipe in (fast, coverage):
        assert "-n auto" in recipe
        assert "$(PYTEST_UNIT_IGNORES)" in recipe
        assert "$(PYTEST_DURATION_ARGS)" in recipe
    assert "PYTEST_UNIT_IGNORES :=" in makefile
    assert "--ignore=tests/repository" in makefile
    assert "--ignore=tests/docs" in makefile
    assert "--ignore=tests/package" in makefile
    assert "--ignore=tests/integration" in makefile
    assert "--ignore=tests/contract" in makefile

    for target in (
        "package-smoke",
        "dbt-polygon-settlement-ci",
        "dbt-match-minute-ci",
        "golden-dbt",
        "contract-http",
        "docs-test",
        "check-repository",
        "dagster-jobs-smoke",
        "dagster-jobs-smoke-cov",
        "dagster-refresh-cov",
        "integration-dbt-serial",
        "integration-dbt-cov-serial",
        "integration-dagster",
    ):
        assert "-n 0" in _target_recipe(makefile, target), target

    parallel = _target_recipe(makefile, "integration-dbt-parallel")
    assert "-n $(DBT_TEST_WORKERS)" in parallel
    assert "test_dbt_incremental_hourly_odds.py" in parallel
    assert _recursive_make_targets(_target_recipe(makefile, "integration-dbt")) == [
        "integration-dbt-parallel",
        "integration-dbt-serial",
    ]
    assert _recursive_make_targets(_target_recipe(makefile, "integration-dbt-cov")) == [
        "integration-dbt-cov-parallel",
        "integration-dbt-cov-serial",
    ]
    assert "DBT_TEST_WORKERS ?=" in makefile
    assert "unit-orchestration:" in makefile
    assert "-n auto" in _target_recipe(makefile, "unit-orchestration")
    assert _recursive_make_targets(
        _target_recipe(makefile, "pipelines-deterministic")
    ) == [
        "integration-dagster",
        "integration-dbt",
        "dbt-polygon-settlement-ci",
        "dbt-match-minute-ci",
        "dbt-minute-odds-ci",
        "dbt-build-ci",
    ]
    pipelines_deterministic = _target_recipe(makefile, "pipelines-deterministic")
    assert pipelines_deterministic.count("-j1") == 6


def test_local_gates_preserve_validation_without_duplicate_parse_or_tests():
    makefile = makefile_text()

    assert _recursive_make_targets(_target_recipe(makefile, "lint")) == [
        "python-lint",
        "dbt-lint",
        "check-repository",
    ]
    assert "-j$(GATE_JOBS)" in _target_recipe(makefile, "ci-fast")
    assert _recursive_make_targets(_target_recipe(makefile, "ci-fast")) == [
        "ci-fast-goal",
    ]
    assert _recursive_make_targets(_target_recipe(makefile, "ci-fast-core")) == [
        "ci-fast-goal",
    ]
    assert "-j1" in _target_recipe(makefile, "ci-fast-core")
    assert _target_prerequisites(makefile, "ci-fast-goal") == [
        "ci-fast-static-docs",
        "ci-fast-tests",
        "ci-fast-dbt",
    ]
    assert "-j$(GATE_JOBS)" in _target_recipe(makefile, "release-gate")
    assert _recursive_make_targets(_target_recipe(makefile, "release-gate")) == [
        "release-gate-goal",
    ]
    assert _recursive_make_targets(_target_recipe(makefile, "release-gate-core")) == [
        "release-gate-goal",
    ]
    assert "-j1" in _target_recipe(makefile, "release-gate-core")
    assert _target_prerequisites(makefile, "release-gate-goal") == [
        "coverage-combine-report",
        "release-gate-costguard-scan",
        "release-gate-mutation",
        "release-gate-static-docs",
    ]
    assert (
        "golden-dbt"
        not in makefile.split("release-gate-goal:", 1)[1].split("\n\n", 1)[0]
    )
    assert _target_prerequisites(makefile, "release-gate-coverage") == [
        "coverage-combine-report",
    ]
    assert _target_prerequisites(makefile, "coverage-combine-report") == [
        "release-gate-cov-unit",
        "release-gate-cov-dagster-jobs",
        "release-gate-cov-dagster-refresh",
        "release-gate-cov-dbt-incremental",
        "release-gate-cov-dbt-serial",
    ]
    assert "COVERAGE_FILE=" in _target_recipe(makefile, "release-gate-cov-unit-run")
    assert _target_prerequisites(makefile, "release-gate-costguard-scan") == [
        "release-gate-dbt-build",
    ]
    assert _target_prerequisites(makefile, "release-gate-dbt-build") == [
        "release-gate-dbt-unit",
        "release-gate-dbt-freshness",
        "release-gate-dbt-polygon",
        "release-gate-dbt-match-order-book",
        "release-gate-dbt-match-minute",
        "release-gate-dbt-minute-odds",
        "release-gate-dbt-market-portrait",
    ]
    assert _target_prerequisites(makefile, "release-gate-dbt-quality") == [
        "release-gate-costguard-scan",
    ]
    # Nested recipes must not force a new jobserver.
    for target in (
        "release-gate-coverage",
        "release-gate-dbt-quality",
        "release-gate-static-docs",
        "release-gate-mutation",
        "coverage-combine-report",
        "release-gate-costguard-scan",
    ):
        recipe = _target_recipe(makefile, target)
        assert not re.search(r"\$\(MAKE\)\s+-j\d+", recipe), target
    assert "--max-children" in _target_recipe(makefile, "mutation")
    assert "MUTMUT_MAX_CHILDREN ?=" in makefile
    assert "GATE_JOBS ?=" in makefile
    assert "DBT_TEST_WORKERS ?=" in makefile
    assert "RELEASE_PYTEST_WORKERS ?=" in makefile
    serial_dbt = _target_recipe(makefile, "integration-dbt-serial")
    assert "tests/integration/duckdb" in serial_dbt
    assert "test_dbt_incremental_hourly_odds.py" in serial_dbt
    assert (
        "--ignore=tests/integration/duckdb/test_dbt_incremental_hourly_odds.py"
        in serial_dbt
    )
    assert "tests/dbt" not in serial_dbt
    assert "bootstrap_dbt_ci_duckdb.py" in _target_recipe(makefile, "dbt-build-ci")
    assert "bootstrap_dbt_ci_duckdb.py" in _target_recipe(makefile, "dbt-unit")
    assert "dbt-prepare:" in makefile
    dbt_prepare = _target_recipe(makefile, "dbt-prepare")
    assert "scripts/dev_loop.py dbt-prepare" in dbt_prepare
    assert "DBT_DEPS_LOCK" in dbt_prepare
    test_dev = _target_recipe(makefile, "test-dev")
    assert "scripts/dev_loop.py polygon-marker" in test_dev
    assert "HYPOTHESIS_PROFILE=dev" in test_dev
    assert "ODDSFOX_RUNTIME_ROOT=" in _target_recipe(makefile, "ci-fast-tests")
    assert "tests/repository" in _target_recipe(makefile, "check-repository")
    terminology = _target_recipe(makefile, "check-terminology")
    assert "tests/repository/test_terminology_policy.py" in terminology
    assert "tests/repository/test_naming_policy.py" in terminology
    assert "-q" in terminology and "-n 0" in terminology
    assert "DBT_LINT_DUCKDB_PATH := $(ODDSFOX_RUNTIME_ROOT)/dbt_lint.duckdb" in makefile
    assert (
        "DBT_BUILD_DUCKDB_PATH := $(ODDSFOX_RUNTIME_ROOT)/dbt_build.duckdb" in makefile
    )

    assert "dbt.cli.main parse" not in _target_recipe(makefile, "format")
    assert "sqlfluff fix" in _target_recipe(makefile, "format")
    assert (
        'scan --manifest "$(ODDSFOX_RUNTIME_DBT_TARGET)/manifest.json"'
        in _target_recipe(makefile, "costguard-scan")
    )


def test_release_gate_match_analysis_lanes_use_isolated_runtime_roots():
    makefile = makefile_text()
    match_order_book = _target_recipe(makefile, "release-gate-dbt-match-order-book")
    match_minute = _target_recipe(makefile, "release-gate-dbt-match-minute")
    market_portrait = _target_recipe(makefile, "release-gate-dbt-market-portrait")
    coverage_prep = _target_recipe(makefile, "release-gate-coverage-prep")

    assert (
        'ODDSFOX_RUNTIME_ROOT="$(RELEASE_DBT_MATCH_ORDER_BOOK_RUNTIME)"'
        in match_order_book
    )
    assert (
        'MATCH_ANALYSIS_RUNTIME_ROOT="$(RELEASE_DBT_MATCH_ORDER_BOOK_RUNTIME)"'
        in match_order_book
    )
    assert 'ODDSFOX_RUNTIME_ROOT="$(RELEASE_DBT_MATCH_MINUTE_RUNTIME)"' in match_minute
    assert (
        'MATCH_ANALYSIS_RUNTIME_ROOT="$(RELEASE_DBT_MATCH_MINUTE_RUNTIME)"'
        in match_minute
    )
    assert (
        'ODDSFOX_RUNTIME_ROOT="$(RELEASE_DBT_MARKET_PORTRAIT_RUNTIME)"'
        in market_portrait
    )
    assert (
        'MATCH_ANALYSIS_RUNTIME_ROOT="$(RELEASE_DBT_MARKET_PORTRAIT_RUNTIME)"'
        in market_portrait
    )
    assert match_order_book != market_portrait
    assert match_minute != match_order_book
    assert (
        'ODDSFOX_RUNTIME_ROOT="$(RELEASE_COVERAGE_RUNTIME)" dbt-prepare'
        in coverage_prep
    )
    assert "release-gate-coverage-prep: dbt-prepare" not in makefile


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
    assert f'UV_CACHE_DIR="{REPO_ROOT}/.cache/runtime/uv"' in recipe
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


def test_export_wc2026_elo_freezes_runs_the_freeze_script():
    proc = subprocess.run(
        ["make", "-n", "export-wc2026-elo-freezes"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    recipe = proc.stdout
    assert "export_eloratings_wc2026_team_ratings_freezes.py" in recipe


def test_export_marts_parquet_runs_the_marts_export_script():
    proc = subprocess.run(
        ["make", "-n", "export-marts-parquet"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    recipe = proc.stdout
    assert "export_marts_parquet.py" in recipe


def test_runtime_and_temporary_storage_default_below_the_checkout():
    makefile = makefile_text()

    assert "ODDSFOX_STORAGE_ROOT ?= $(REPO_ROOT)" in makefile
    assert "ODDSFOX_RUNTIME_ROOT ?= $(REPO_ROOT)/.cache/runtime" in makefile
    assert "export TMPDIR := $(ODDSFOX_RUNTIME_TMP)" in makefile
    assert "export TMP := $(ODDSFOX_RUNTIME_TMP)" in makefile
    assert "export TEMP := $(ODDSFOX_RUNTIME_TMP)" in makefile
    assert "export XDG_CACHE_HOME := $(ODDSFOX_RUNTIME_XDG)" in makefile
    assert "export UV_CACHE_DIR := $(ODDSFOX_RUNTIME_UV)" in makefile
    assert "export UV_PYTHON_INSTALL_DIR := $(ODDSFOX_RUNTIME_UV_PYTHON)" in makefile
    assert "ODDSFOX_RUNTIME_UV := $(REPO_ROOT)/.cache/runtime/uv" in makefile
    assert (
        "ODDSFOX_RUNTIME_UV_PYTHON := $(REPO_ROOT)/.cache/runtime/uv-python" in makefile
    )
    assert (
        "ODDSFOX_RUNTIME_PLAYWRIGHT := $(REPO_ROOT)/.cache/runtime/ms-playwright"
        in makefile
    )
    assert (
        "export PLAYWRIGHT_BROWSERS_PATH := $(ODDSFOX_RUNTIME_PLAYWRIGHT)" in makefile
    )
    assert "export DUCKDB_EXTENSION_DIRECTORY" not in makefile
    assert (
        'DUCKDB_EXTENSION_DIRECTORY="$(ODDSFOX_RUNTIME_DUCKDB_EXTENSIONS)"' in makefile
    )
    assert "warehouse paths must remain below SSD-backed ODDSFOX_STORAGE_ROOT" in (
        makefile
    )
