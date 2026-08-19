"""Keep private collection and portrait rendering out of the public pipeline."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_check


ROOT = Path(__file__).resolve().parents[2]
SELF = "tests/repository/test_repository_boundaries.py"
TEXT_SUFFIXES = {
    ".css",
    ".html",
    ".js",
    ".jsx",
    ".json",
    ".md",
    ".py",
    ".sql",
    ".toml",
    ".ts",
    ".tsx",
    ".yaml",
    ".yml",
}
FORBIDDEN = (
    "fotmob",
    "three.js",
    "effectcomposer",
    "buffergeometry",
    "window.renderframe",
    "ffmpeg",
)


def test_public_pipeline_has_no_private_collection_or_rendering_implementation() -> (
    None
):
    tracked = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    violations: list[str] = []
    for relative in tracked:
        path = ROOT / relative
        if relative == SELF or path.suffix not in TEXT_SUFFIXES or not path.is_file():
            continue
        content = path.read_text(encoding="utf-8", errors="ignore").casefold()
        violations.extend(
            f"{relative}: {token}" for token in FORBIDDEN if token in content
        )
    assert not violations, "repository boundary violations:\n" + "\n".join(violations)
