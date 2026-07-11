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
