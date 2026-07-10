"""Tests for scripts/run_scope.py."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest


def _load_runner_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import run_scope

    return run_scope


def test_run_scope_lists_known_scopes(capsys):
    runner = _load_runner_module()

    assert runner.main(["--list"]) == 0

    out = capsys.readouterr().out
    assert "polymarket:wc2026\tpolymarket_wc2026" in out
    assert "polymarket:us_midterms_2026\tpolymarket_us_midterms_2026" in out
    assert "kalshi:wc2026\tkalshi_wc2026" in out


def test_run_scope_dry_run_accepts_multiple_scope_aliases(capsys):
    runner = _load_runner_module()

    assert (
        runner.main(
            [
                "polymarket_wc2026",
                "kalshi:wc2026",
                "--step",
                "dbt",
                "--dry-run",
                "--python",
                "/usr/bin/python3",
            ]
        )
        == 0
    )

    lines = capsys.readouterr().out.splitlines()
    assert lines == [
        "/usr/bin/python3 -m dagster job execute -m "
        "oddsfox_pipeline.orchestration.definitions -j polymarket_wc2026_dbt_build",
        "/usr/bin/python3 -m dagster job execute -m "
        "oddsfox_pipeline.orchestration.definitions -j kalshi_wc2026_dbt_build",
    ]


def test_run_scope_executes_fixed_dagster_job():
    runner = _load_runner_module()

    with patch.object(
        runner.subprocess,
        "run",
        return_value=SimpleNamespace(returncode=0),
    ) as run:
        assert (
            runner.main(
                [
                    "polymarket:us_midterms_2026",
                    "--step",
                    "odds",
                    "--python",
                    "/usr/bin/python3",
                ]
            )
            == 0
        )

    assert run.call_args.kwargs["cwd"] == runner.REPO_ROOT
    assert run.call_args.kwargs["check"] is False
    assert run.call_args.args[0] == [
        "/usr/bin/python3",
        "-m",
        "dagster",
        "job",
        "execute",
        "-m",
        "oddsfox_pipeline.orchestration.definitions",
        "-j",
        "polymarket_us_midterms_2026_hourly_odds_ingest",
    ]


def test_run_scope_fails_fast_by_default():
    runner = _load_runner_module()

    with patch.object(
        runner.subprocess,
        "run",
        return_value=SimpleNamespace(returncode=7),
    ) as run:
        assert (
            runner.main(
                [
                    "polymarket:wc2026",
                    "kalshi:wc2026",
                    "--step",
                    "registry",
                ]
            )
            == 7
        )

    assert run.call_count == 1


def test_run_scope_can_continue_after_failure():
    runner = _load_runner_module()

    with patch.object(
        runner.subprocess,
        "run",
        side_effect=[
            SimpleNamespace(returncode=7),
            SimpleNamespace(returncode=0),
        ],
    ) as run:
        assert (
            runner.main(
                [
                    "polymarket:wc2026",
                    "kalshi:wc2026",
                    "--continue-on-error",
                ]
            )
            == 7
        )

    assert run.call_count == 2


def test_run_scope_rejects_missing_and_unknown_scope():
    runner = _load_runner_module()

    with pytest.raises(SystemExit):
        runner.main([])

    with pytest.raises(SystemExit):
        runner.main(["polymarket:missing"])
