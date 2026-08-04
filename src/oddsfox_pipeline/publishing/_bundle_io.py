"""Shared filesystem hashing, checksum, and release revision helpers."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

SEMVER_RE: Final = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*["
    r"a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)
COMMIT_RE: Final = re.compile(r"^[0-9a-f]{40}$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_text(path: Path, text: str) -> None:
    path.write_text(text.rstrip() + "\n", encoding="utf-8", newline="\n")


def write_json(
    path: Path,
    value: Any,
    *,
    jsonable: Callable[[Any], Any] | None = None,
) -> None:
    payload = jsonable(value) if jsonable is not None else value
    write_text(
        path,
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
    )


def write_checksums(directory: Path, *, file_names: set[str]) -> None:
    lines = [
        f"{sha256_file(directory / name)}  {name}"
        for name in sorted(file_names - {"CHECKSUMS.sha256"})
    ]
    write_text(directory / "CHECKSUMS.sha256", "\n".join(lines))


def validate_dataset_version(value: str) -> str:
    if not SEMVER_RE.fullmatch(value):
        raise ValueError(f"dataset_version must be SemVer 2.0, got {value!r}")
    return value


def git_head_sha(
    repo_root: Path,
    *,
    resolve_error: str = "Could not resolve the generator Git commit",
    invalid_error: str = "Git returned an invalid generator commit",
) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(resolve_error) from exc
    commit = completed.stdout.strip().lower()
    if not COMMIT_RE.fullmatch(commit):
        raise RuntimeError(invalid_error)
    return commit


def current_clean_commit(
    repo_root: Path,
    *,
    untracked_files: str = "normal",
    dirty_error: str = "Dataset releases require a clean Git working tree",
    resolve_error: str = "Could not resolve the generator Git commit",
    invalid_error: str = "Git returned an invalid generator commit",
) -> str:
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", f"--untracked-files={untracked_files}"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(resolve_error) from exc
    if status.stdout.strip():
        raise RuntimeError(dirty_error)
    return git_head_sha(
        repo_root,
        resolve_error=resolve_error,
        invalid_error=invalid_error,
    )


__all__ = [
    "COMMIT_RE",
    "SEMVER_RE",
    "current_clean_commit",
    "git_head_sha",
    "sha256_file",
    "validate_dataset_version",
    "write_checksums",
    "write_json",
    "write_text",
]
