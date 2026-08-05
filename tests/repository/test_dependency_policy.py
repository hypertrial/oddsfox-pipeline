"""Dependency-graph policy: pandas must stay out of the resolved lock."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parents[2]
_PACKAGE_NAME = re.compile(r'(?m)^name = "([^"]+)"')


def test_no_pandas_anywhere_in_resolved_dependency_graph() -> None:
    lock_text = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    packages = {name.casefold() for name in _PACKAGE_NAME.findall(lock_text)}
    assert "pandas" not in packages, (
        "pandas must not appear in uv.lock, even transitively; "
        "this repo standardizes on polars (see AGENTS.md)"
    )
