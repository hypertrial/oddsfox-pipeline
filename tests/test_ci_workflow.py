"""GitHub Actions workflow structure checks."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_ci_workflow_keeps_parallel_fast_lane_contract():
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()

    for job in (
        "detect-changes",
        "docs-policy",
        "lint",
        "unit-coverage",
        "dagster-jobs-coverage",
        "dagster-refresh-coverage",
        "dbt-integration-coverage",
        "dbt-project-tests",
        "dbt-build-quality",
        "coverage-report",
        "ci-success",
    ):
        assert f"  {job}:" in workflow

    assert "docs_only: ${{ steps.filter.outputs.docs_only }}" in workflow
    assert "full_ci: ${{ steps.filter.outputs.full_ci }}" in workflow
    assert "docs/*|README.md|CHANGELOG.md|CONTRIBUTING.md" in workflow
    assert "uv run make gx-data-quality" in workflow
    assert "uv run make dagster-jobs-smoke-cov" in workflow
    assert "uv run make dagster-refresh-cov" in workflow
    assert "include-hidden-files: true" in workflow
    assert "uv run python -m coverage combine .coverage-artifacts" in workflow
    assert "Check required jobs" in workflow
    assert "actions/checkout@v4" not in workflow
    assert "actions/setup-python@v5" not in workflow
    assert "astral-sh/setup-uv@v5" not in workflow
    assert "actions/upload-artifact@v4" not in workflow
    assert "actions/download-artifact@v4" not in workflow
    assert "actions/checkout@v7" in workflow
    assert "actions/setup-python@v6" in workflow
    assert "astral-sh/setup-uv@v8.3.2" in workflow
    assert "actions/upload-artifact@v7" in workflow
    assert "actions/download-artifact@v8" in workflow


def test_live_readiness_is_manual_disposable_and_diagnostics_only():
    workflow = (REPO_ROOT / ".github" / "workflows" / "live-readiness.yml").read_text()

    assert "workflow_dispatch:" in workflow
    assert "pull_request:" not in workflow
    assert "push:" not in workflow
    assert "uv run make contract-http" in workflow
    assert "uv run make live-smoke" in workflow
    assert "DUCKDB_PATH: .cache/live-readiness/oddsfox.duckdb" in workflow
    assert ".cache/live-readiness/run.log" in workflow
    assert ".cache/live-readiness/exit-status" in workflow
    assert "*.duckdb" not in workflow
