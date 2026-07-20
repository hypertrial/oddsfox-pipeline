"""GitHub Actions workflow structure checks."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_ci_workflows_keep_publication_manual_and_permissions_scoped():
    workflow_dir = REPO_ROOT / ".github" / "workflows"
    automatic_path = workflow_dir / "ci.yml"
    manual_path = workflow_dir / "manual-full.yml"
    automatic = yaml.safe_load(automatic_path.read_text())
    manual = yaml.safe_load(manual_path.read_text())
    automatic_text = automatic_path.read_text()
    manual_text = manual_path.read_text()

    assert set(automatic["jobs"]) == {"fast-gate"}
    assert automatic["jobs"]["fast-gate"]["timeout-minutes"] == 5
    assert "uv run make ci-fast" in automatic_text
    assert "docker/build-push-action" not in automatic_text
    assert "push: true" not in automatic_text

    assert set(manual["jobs"]) == {"full-gate", "publish"}
    assert manual["permissions"] == {"contents": "read"}
    assert manual["jobs"]["publish"]["permissions"] == {
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }
    assert "uv run make release-gate-core" in manual_text
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
    assert "live-smoke" not in automatic_text + manual_text
    assert "source-audit" not in automatic_text + manual_text
    assert not (workflow_dir / "live-readiness.yml").exists()
    assert sorted(path.name for path in workflow_dir.glob("*.yml")) == [
        "ci.yml",
        "manual-full.yml",
    ]
