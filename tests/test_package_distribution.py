"""Built-wheel licensing and distribution checks."""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_built_wheel_declares_mit_and_contains_notices(tmp_path: Path) -> None:
    completed = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    wheels = list(tmp_path.glob("*.whl"))
    assert len(wheels) == 1

    with zipfile.ZipFile(wheels[0]) as archive:
        names = archive.namelist()
        metadata_name = next(name for name in names if name.endswith("/METADATA"))
        metadata = archive.read(metadata_name).decode()

    assert "License-Expression: MIT" in metadata
    assert any(name.endswith(".dist-info/licenses/LICENSE") for name in names)
    assert any(
        name.endswith(".dist-info/licenses/THIRD_PARTY_NOTICES.md") for name in names
    )
    assert not any(
        name.endswith((".csv", ".db", ".duckdb", ".parquet", ".pdf")) for name in names
    )
