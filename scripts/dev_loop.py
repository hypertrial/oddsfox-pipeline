#!/usr/bin/env python3
"""Dev-loop helpers: incremental dbt prepare and Polygon area deselection."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# ponytail: shared-infra list is manually maintained; upgrade path is pytest-testmon
# or coverage-based selection if a new cross-cutting module causes false skips.
_SHARED_INFRA_PREFIXES: tuple[str, ...] = (
    "pyproject.toml",
    "uv.lock",
    "Makefile",
    "tests/conftest.py",
    "tests/support/",
    "dbt/dbt_project.yml",
    "dbt/packages.yml",
    "dbt/profiles/",
    "src/oddsfox_pipeline/config/",
    "src/oddsfox_pipeline/storage/duckdb/connection.py",
    "src/oddsfox_pipeline/storage/duckdb/schemas/",
    "src/oddsfox_pipeline/resources/",
    ".github/workflows/",
)

_DBT_HASH_ROOTS: tuple[str, ...] = (
    "dbt/models",
    "dbt/macros",
    "dbt/seeds",
    "dbt/tests",
)
_DBT_HASH_FILES: tuple[str, ...] = (
    "dbt/dbt_project.yml",
    "dbt/packages.yml",
    "dbt/profiles/profiles.yml",
)

# ponytail: 7-day fixed retention for stale dbt partial-parse dirs; no config knob.
_STALE_DBT_DIR_MAX_AGE_SECONDS = 7 * 24 * 60 * 60


def _run_git(
    args: list[str], *, cwd: Path = REPO_ROOT
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def _is_shared_infra_change(path: str) -> bool:
    normalized = path.replace("\\", "/")
    for prefix in _SHARED_INFRA_PREFIXES:
        if normalized == prefix or normalized.startswith(prefix):
            return True
    return False


def _is_polygon_change(path: str) -> bool:
    return "polygon" in path.lower()


def polygon_marker(*, base_ref: str) -> str:
    """Return a pytest marker suffix, or empty string when uncertain."""
    merge_base = _run_git(["merge-base", "HEAD", base_ref])
    if merge_base.returncode != 0 or not merge_base.stdout.strip():
        return ""

    diff = _run_git(["diff", "--name-only", merge_base.stdout.strip()])
    if diff.returncode != 0:
        return ""

    changed = [line.strip() for line in diff.stdout.splitlines() if line.strip()]
    if not changed:
        return "and not polygon"

    for path in changed:
        if _is_polygon_change(path) or _is_shared_infra_change(path):
            return ""
    return "and not polygon"


def _iter_dbt_hash_entries(repo_root: Path) -> list[tuple[str, int, float]]:
    entries: list[tuple[str, int, float]] = []
    for rel_root in _DBT_HASH_ROOTS:
        root = repo_root / rel_root
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            stat = path.stat()
            entries.append(
                (path.relative_to(repo_root).as_posix(), stat.st_size, stat.st_mtime)
            )
    for rel in _DBT_HASH_FILES:
        path = repo_root / rel
        if not path.is_file():
            continue
        stat = path.stat()
        entries.append((rel, stat.st_size, stat.st_mtime))
    return entries


def _compute_dbt_fingerprint(repo_root: Path) -> str:
    # ponytail: mtime+size fingerprint, not content hash; upgrade path is content
    # hashing if mtime-preserving edits ever bite.
    digest = hashlib.sha256()
    for rel, size, mtime in _iter_dbt_hash_entries(repo_root):
        digest.update(f"{rel}\0{size}\0{mtime}\n".encode())
    return digest.hexdigest()


def _prune_stale_dbt_dirs(target_path: Path) -> None:
    if not target_path.is_dir():
        return
    cutoff = time.time() - _STALE_DBT_DIR_MAX_AGE_SECONDS
    for child in target_path.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if not (name.startswith("oddsfox_dbt") and "-" in name):
            continue
        try:
            if child.stat().st_mtime < cutoff:
                shutil.rmtree(child)
        except OSError:
            continue


def _dbt_prepare_needed(
    *,
    repo_root: Path,
    target_path: Path,
    project_dir: Path,
) -> bool:
    stamp_path = target_path / ".dbt_prepare_stamp"
    manifest_path = target_path / "manifest.json"
    packages_path = project_dir / "dbt_packages"
    if not manifest_path.is_file() or not packages_path.is_dir():
        return True
    if not stamp_path.is_file():
        return True
    current = _compute_dbt_fingerprint(repo_root)
    return stamp_path.read_text(encoding="utf-8").strip() != current


def _write_dbt_stamp(*, repo_root: Path, target_path: Path) -> None:
    target_path.mkdir(parents=True, exist_ok=True)
    stamp_path = target_path / ".dbt_prepare_stamp"
    stamp_path.write_text(_compute_dbt_fingerprint(repo_root), encoding="utf-8")


def dbt_prepare(
    *,
    repo_root: Path,
    target_path: Path,
    project_dir: Path,
    profiles_dir: Path,
    deps_lock: Path,
) -> None:
    _prune_stale_dbt_dirs(target_path)
    if not _dbt_prepare_needed(
        repo_root=repo_root,
        target_path=target_path,
        project_dir=project_dir,
    ):
        print("dbt project unchanged; skipping deps/parse")
        return

    deps_lock.parent.mkdir(parents=True, exist_ok=True)
    with deps_lock.open("w", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "dbt.cli.main",
                "deps",
                "--quiet",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(profiles_dir),
            ],
            cwd=repo_root,
        )
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "parse",
            "--quiet",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
        ],
        cwd=repo_root,
    )
    _write_dbt_stamp(repo_root=repo_root, target_path=target_path)


def _cmd_polygon_marker(args: argparse.Namespace) -> int:
    print(polygon_marker(base_ref=args.base_ref), end="")
    return 0


def _cmd_dbt_prepare(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo_root).resolve()
    dbt_prepare(
        repo_root=repo_root,
        target_path=Path(args.target_path).resolve(),
        project_dir=(repo_root / args.project_dir).resolve(),
        profiles_dir=(repo_root / args.profiles_dir).resolve(),
        deps_lock=Path(args.deps_lock).resolve(),
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    polygon_parser = subparsers.add_parser(
        "polygon-marker",
        help="Print pytest marker suffix to deselect Polygon tests when safe",
    )
    polygon_parser.add_argument(
        "--base-ref",
        default="origin/main",
        help="Base ref for merge-base diff (default: origin/main)",
    )
    polygon_parser.set_defaults(func=_cmd_polygon_marker)

    prepare_parser = subparsers.add_parser(
        "dbt-prepare",
        help="Run dbt deps/parse only when the dbt project fingerprint changed",
    )
    prepare_parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root (default: parent of scripts/)",
    )
    prepare_parser.add_argument(
        "--target-path",
        required=True,
        help="dbt target directory (manifest.json parent)",
    )
    prepare_parser.add_argument(
        "--project-dir",
        default="dbt",
        help="dbt project directory relative to repo root",
    )
    prepare_parser.add_argument(
        "--profiles-dir",
        default="dbt/profiles",
        help="dbt profiles directory relative to repo root",
    )
    prepare_parser.add_argument(
        "--deps-lock",
        default=str(REPO_ROOT / ".cache/runtime/dbt-deps.lock"),
        help="fcntl lock file for serialized dbt deps",
    )
    prepare_parser.set_defaults(func=_cmd_dbt_prepare)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
