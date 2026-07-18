"""GitHub Actions workflow structure checks."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_ci_workflow_keeps_bounded_offline_gate_and_signed_image_release():
    workflow_path = REPO_ROOT / ".github" / "workflows" / "ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text())
    workflow_text = workflow_path.read_text()

    assert set(workflow["jobs"]) == {"fast-gate", "container", "publish"}
    assert workflow["jobs"]["fast-gate"]["timeout-minutes"] == 5
    assert "uv run make lint test contract-http dbt-parse docs-build" in workflow_text
    assert "live-smoke" not in workflow_text
    assert "source-audit" not in workflow_text
    assert "actions/checkout@v4" not in workflow_text
    assert "ghcr.io/hypertrial/oddsfox-pipeline" in workflow_text
    assert "linux/amd64,linux/arm64" in workflow_text
    assert "provenance: mode=max" in workflow_text
    assert "sbom: true" in workflow_text
    assert "cosign sign --yes" in workflow_text
    assert not (workflow_path.parent / "live-readiness.yml").exists()
    assert sorted(path.name for path in workflow_path.parent.glob("*.yml")) == [
        "ci.yml"
    ]
