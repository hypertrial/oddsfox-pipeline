"""GitHub Actions workflow structure checks."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEDULE_FLAGS = {
    "POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED": "false",
    "POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED": "false",
    "KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED": "false",
    "WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED": "false",
}


def _make_targets(job: dict) -> list[str]:
    prefix = "uv run make "
    return [
        step["run"].removeprefix(prefix)
        for step in job["steps"]
        if step.get("run", "").startswith(prefix)
    ]


def _assert_pinned_actions(workflow: dict) -> None:
    for job in workflow["jobs"].values():
        for step in job["steps"]:
            if action := step.get("uses"):
                assert re.search(r"@[0-9a-f]{40}$", action), action


def _uv_sync_command(job: dict) -> str:
    sync_commands = [
        step["run"]
        for step in job["steps"]
        if isinstance(step.get("run"), str) and step["run"].startswith("uv sync ")
    ]
    assert len(sync_commands) == 1, sync_commands
    return sync_commands[0]


def _assert_python_worker(job: dict, timeout: int, *, sync_command: str) -> None:
    assert job["timeout-minutes"] == timeout
    assert [step["uses"] for step in job["steps"] if "uses" in step][:3] == [
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065",
        "astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39",
    ]
    checkout = next(
        step
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/checkout")
    )
    assert checkout["with"]["persist-credentials"] is False
    assert _uv_sync_command(job) == sync_command
    assert "--extra dev" not in sync_command
    assert "--no-default-groups" in sync_command


def test_ci_workflows_keep_publication_manual_and_permissions_scoped():
    workflow_dir = REPO_ROOT / ".github" / "workflows"
    automatic_path = workflow_dir / "ci.yml"
    manual_path = workflow_dir / "manual-full.yml"
    automatic = yaml.safe_load(automatic_path.read_text())
    manual = yaml.safe_load(manual_path.read_text())
    automatic_text = automatic_path.read_text()
    manual_text = manual_path.read_text()

    assert automatic["permissions"] == {"contents": "read"}
    assert {key: automatic["env"][key] for key in SCHEDULE_FLAGS} == SCHEDULE_FLAGS
    assert set(automatic["jobs"]) == {
        "static-docs",
        "tests",
        "dbt",
        "python-compat",
        "fast-gate",
    }
    assert _uv_sync_command(automatic["jobs"]["static-docs"]) == (
        "uv sync --frozen --no-default-groups --group test --group python-lint --group docs"
    )
    assert _uv_sync_command(automatic["jobs"]["tests"]) == (
        "uv sync --frozen --no-default-groups --group test"
    )
    assert _uv_sync_command(automatic["jobs"]["dbt"]) == (
        "uv sync --frozen --no-default-groups --group dbt-lint"
    )
    assert _uv_sync_command(automatic["jobs"]["python-compat"]) == (
        "uv sync --frozen --no-default-groups --group test"
    )
    for worker, sync_command in (
        (
            "static-docs",
            "uv sync --frozen --no-default-groups --group test --group python-lint --group docs",
        ),
        ("tests", "uv sync --frozen --no-default-groups --group test"),
        ("dbt", "uv sync --frozen --no-default-groups --group dbt-lint"),
        ("python-compat", "uv sync --frozen --no-default-groups --group test"),
    ):
        _assert_python_worker(automatic["jobs"][worker], 8, sync_command=sync_command)
    assert all(
        next(
            step["with"]["python-version"]
            for step in automatic["jobs"][worker]["steps"]
            if step.get("uses", "").startswith("actions/setup-python")
        )
        == "3.10"
        for worker in ("static-docs", "tests", "dbt")
    )
    assert (
        next(
            step["with"]["python-version"]
            for step in automatic["jobs"]["python-compat"]["steps"]
            if step.get("uses", "").startswith("actions/setup-python")
        )
        == "3.13"
    )
    assert _make_targets(automatic["jobs"]["static-docs"]) == [
        "python-lint",
        "check-repository",
        "docs-build",
    ]
    assert _make_targets(automatic["jobs"]["tests"]) == ["test", "contract-http"]
    assert _make_targets(automatic["jobs"]["dbt"]) == ["dbt-lint"]
    assert _make_targets(automatic["jobs"]["python-compat"]) == [
        "package-smoke",
        "test",
    ]

    fast_gate = automatic["jobs"]["fast-gate"]
    assert fast_gate["if"] == "always()"
    assert set(fast_gate["needs"]) == {
        "static-docs",
        "tests",
        "dbt",
        "python-compat",
    }
    assert fast_gate["timeout-minutes"] == 8
    fast_gate_command = fast_gate["steps"][0]["run"]
    assert all(
        f"needs.{worker}.result" in fast_gate_command
        for worker in ("static-docs", "tests", "dbt", "python-compat")
    )
    assert "uv run make ci-fast" not in automatic_text
    assert "dbt-parse" not in automatic_text
    assert "docker/build-push-action" not in automatic_text
    assert "push: true" not in automatic_text

    assert set(manual["jobs"]) == {
        "coverage",
        "dbt-quality",
        "mutation",
        "static-docs",
        "full-gate",
    }
    assert manual["permissions"] == {"contents": "read"}
    # PyYAML parses the workflow `on:` key as boolean True.
    assert list(manual[True]) == ["workflow_dispatch"]
    assert not manual[True]["workflow_dispatch"]
    assert {key: manual["env"][key] for key in SCHEDULE_FLAGS} == SCHEDULE_FLAGS
    for worker, sync_command in (
        ("coverage", "uv sync --frozen --no-default-groups --group coverage"),
        (
            "dbt-quality",
            "uv sync --frozen --no-default-groups --group test --group dbt-lint",
        ),
        ("mutation", "uv sync --frozen --no-default-groups --group mutation"),
        (
            "static-docs",
            "uv sync --frozen --no-default-groups --group test --group python-lint --group dbt-lint --group docs-render",
        ),
    ):
        _assert_python_worker(manual["jobs"][worker], 45, sync_command=sync_command)
    assert _make_targets(manual["jobs"]["coverage"]) == [
        '-j"$GATE_JOBS" release-gate-coverage'
    ]
    assert _make_targets(manual["jobs"]["dbt-quality"]) == [
        '-j"$GATE_JOBS" release-gate-dbt-quality'
    ]
    assert "golden-dbt" not in manual_text
    playwright_cache = next(
        step
        for step in manual["jobs"]["static-docs"]["steps"]
        if step.get("name") == "Cache Playwright browsers"
    )
    assert playwright_cache["uses"].startswith("actions/cache@")
    assert (
        playwright_cache["with"]["path"]
        == "${{ github.workspace }}/.cache/runtime/ms-playwright"
    )
    assert "hashFiles('uv.lock')" in playwright_cache["with"]["key"]
    assert _make_targets(manual["jobs"]["mutation"]) == ["mutation-ci"]
    mutation_export = manual["jobs"]["mutation"]["steps"][-2]
    assert mutation_export == {
        "name": "Export mutation statistics",
        "if": "always()",
        "run": "uv run mutmut export-cicd-stats",
    }
    mutation_artifact = manual["jobs"]["mutation"]["steps"][-1]
    assert mutation_artifact == {
        "name": "Upload mutation statistics",
        "if": "always()",
        "uses": ("actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"),
        "with": {
            "name": "mutmut-cicd-stats",
            "path": "mutants/mutmut-cicd-stats.json",
            "if-no-files-found": "error",
            "retention-days": 7,
        },
    }
    assert _make_targets(manual["jobs"]["static-docs"]) == [
        "python-lint",
        "dbt-lint",
        "check-repository",
        "package-smoke",
        "contract-http",
        "docs-build",
        "docs-test",
    ]
    assert manual["env"]["GATE_JOBS"] == "2"

    full_gate = manual["jobs"]["full-gate"]
    assert full_gate["if"] == "always()"
    assert set(full_gate["needs"]) == {
        "coverage",
        "dbt-quality",
        "mutation",
        "static-docs",
    }
    assert full_gate["timeout-minutes"] == 1
    full_gate_command = full_gate["steps"][0]["run"]
    assert all(
        f"needs.{worker}.result" in full_gate_command
        for worker in ("coverage", "dbt-quality", "mutation", "static-docs")
    )

    assert "publish" not in manual["jobs"]
    assert "uv run make release-gate-core" not in manual_text
    assert "docker/" not in manual_text
    assert "ghcr.io" not in manual_text
    assert "cosign" not in manual_text
    assert "container-smoke" not in manual_text
    _assert_pinned_actions(automatic)
    _assert_pinned_actions(manual)
    assert "live-smoke" not in automatic_text + manual_text
    assert "source-audit" not in automatic_text + manual_text
    assert not (workflow_dir / "live-readiness.yml").exists()
    assert sorted(path.name for path in workflow_dir.glob("*.yml")) == [
        "ci.yml",
        "manual-full.yml",
    ]
