"""Inline Makefile include fragments for repository tests."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_INCLUDE_PATTERN = re.compile(r"^include\s+([^\s#]+)\s*$", re.MULTILINE)


def makefile_text(repo_root: Path | None = None) -> str:
    """Return the root Makefile with all ``include`` fragments inlined."""
    root = repo_root or REPO_ROOT
    return _inline_includes((root / "Makefile").read_text(encoding="utf-8"), root)


def _inline_includes(text: str, root: Path) -> str:
    while True:
        match = _INCLUDE_PATTERN.search(text)
        if match is None:
            return text
        include_path = root / match.group(1)
        fragment = include_path.read_text(encoding="utf-8")
        if not fragment.endswith("\n"):
            fragment += "\n"
        text = text[: match.start()] + fragment + text[match.end() :]
