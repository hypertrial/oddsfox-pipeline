"""GitHub Actions workflow structure checks."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
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


def _assert_python_worker(job: dict, timeout: int) -> None:
    assert job["timeout-minutes"] == timeout
    assert [step["uses"] for step in job["steps"] if "uses" in step][:3] == [
        "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065",
        "astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39",
    ]
    checkout = next(
        step
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/checkout")
    )
    assert checkout["with"]["persist-credentials"] is False
    assert "uv sync --frozen --extra dev" in [step.get("run") for step in job["steps"]]


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
    assert set(automatic["jobs"]) == {"static-docs", "tests", "dbt", "fast-gate"}
    for worker in ("static-docs", "tests", "dbt"):
        _assert_python_worker(automatic["jobs"][worker], 8)
    assert _make_targets(automatic["jobs"]["static-docs"]) == [
        "python-lint",
        "check-secrets",
        "check-distribution",
        "docs-build",
    ]
    assert _make_targets(automatic["jobs"]["tests"]) == ["test", "contract-http"]
    assert _make_targets(automatic["jobs"]["dbt"]) == ["dbt-lint"]

    fast_gate = automatic["jobs"]["fast-gate"]
    assert fast_gate["if"] == "always()"
    assert set(fast_gate["needs"]) == {"static-docs", "tests", "dbt"}
    assert fast_gate["timeout-minutes"] == 8
    fast_gate_command = fast_gate["steps"][0]["run"]
    assert all(
        f"needs.{worker}.result" in fast_gate_command
        for worker in ("static-docs", "tests", "dbt")
    )
    assert "uv run make ci-fast" not in automatic_text
    assert "dbt-parse" not in automatic_text
    assert "docker/build-push-action" not in automatic_text
    assert "push: true" not in automatic_text

    assert set(manual["jobs"]) == {
        "coverage",
        "dbt-quality",
        "static-docs-container",
        "full-gate",
        "publish",
    }
    assert manual["permissions"] == {"contents": "read"}
    assert {key: manual["env"][key] for key in SCHEDULE_FLAGS} == SCHEDULE_FLAGS
    for worker in ("coverage", "dbt-quality", "static-docs-container"):
        _assert_python_worker(manual["jobs"][worker], 45)
    assert _make_targets(manual["jobs"]["coverage"]) == [
        "test-cov",
        "dagster-jobs-smoke-cov",
        "dagster-refresh-cov",
        "integration-dbt-cov",
        "coverage-report",
    ]
    assert _make_targets(manual["jobs"]["dbt-quality"]) == [
        "dbt-unit",
        "golden-dbt",
        "dbt-source-freshness-ci",
        "dbt-polygon-settlement-ci",
        "dbt-build-ci",
        "gx-data-quality",
        "costguard-scan",
    ]
    assert _make_targets(manual["jobs"]["static-docs-container"]) == [
        "python-lint",
        "dbt-lint",
        "check-secrets",
        "check-distribution",
        "package-smoke",
        "contract-http",
        "docs-build",
        "docs-test",
        "container-smoke-run",
    ]

    full_gate = manual["jobs"]["full-gate"]
    assert full_gate["if"] == "always()"
    assert set(full_gate["needs"]) == {
        "coverage",
        "dbt-quality",
        "static-docs-container",
    }
    assert full_gate["timeout-minutes"] == 1
    full_gate_command = full_gate["steps"][0]["run"]
    assert all(
        f"needs.{worker}.result" in full_gate_command
        for worker in ("coverage", "dbt-quality", "static-docs-container")
    )

    assert manual["jobs"]["publish"]["needs"] == "full-gate"
    assert manual["jobs"]["publish"]["timeout-minutes"] == 20
    assert manual["jobs"]["publish"]["permissions"] == {
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }
    assert "uv run make release-gate-core" not in manual_text
    assert "PUBLISH_IMAGE: ghcr.io/hypertrial/oddsfox-pipeline" in manual_text
    assert "tags: oddsfox-pipeline:ci" in manual_text
    assert "uv run make container-smoke-run" in manual_text
    assert "inputs.publish && github.ref == 'refs/heads/main'" in manual_text
    assert "linux/amd64,linux/arm64" in manual_text
    assert "provenance: mode=max" in manual_text
    assert "sbom: true" in manual_text
    assert "cosign sign --yes" in manual_text
    assert "create-storage-record: false" in manual_text
    assert "scope=pipeline-image" in manual_text
    _assert_pinned_actions(automatic)
    _assert_pinned_actions(manual)
    assert "live-smoke" not in automatic_text + manual_text
    assert "source-audit" not in automatic_text + manual_text
    assert not (workflow_dir / "live-readiness.yml").exists()
    assert sorted(path.name for path in workflow_dir.glob("*.yml")) == [
        "ci.yml",
        "manual-full.yml",
    ]
