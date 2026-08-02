"""Unit checks for scripts/dev_loop.py selection helpers."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
sys.path.insert(0, str(scripts_dir))
import dev_loop  # noqa: E402


def test_polygon_marker_skips_when_diff_empty(monkeypatch):
    monkeypatch.setattr(
        dev_loop,
        "_run_git",
        lambda args, cwd=None: subprocess.CompletedProcess(
            args, 0, "abc123\n" if args[0] == "merge-base" else "", ""
        ),
    )
    assert dev_loop.polygon_marker(base_ref="origin/main") == "and not polygon"


def test_polygon_marker_skips_non_polygon_branch_changes(monkeypatch):
    def fake_git(args, cwd=None):
        if args[0] == "merge-base":
            return subprocess.CompletedProcess(args, 0, "abc123\n", "")
        return subprocess.CompletedProcess(
            args,
            0,
            "src/oddsfox_pipeline/ingestion/polymarket/markets/sync.py\n",
            "",
        )

    monkeypatch.setattr(dev_loop, "_run_git", fake_git)
    assert dev_loop.polygon_marker(base_ref="origin/main") == "and not polygon"


def test_polygon_marker_keeps_polygon_when_polygon_path_changes(monkeypatch):
    def fake_git(args, cwd=None):
        if args[0] == "merge-base":
            return subprocess.CompletedProcess(args, 0, "abc123\n", "")
        return subprocess.CompletedProcess(
            args,
            0,
            "src/oddsfox_pipeline/ingestion/polymarket/polygon_seed.py\n",
            "",
        )

    monkeypatch.setattr(dev_loop, "_run_git", fake_git)
    assert dev_loop.polygon_marker(base_ref="origin/main") == ""


def test_polygon_marker_keeps_polygon_when_shared_infra_changes(monkeypatch):
    def fake_git(args, cwd=None):
        if args[0] == "merge-base":
            return subprocess.CompletedProcess(args, 0, "abc123\n", "")
        return subprocess.CompletedProcess(args, 0, "Makefile\n", "")

    monkeypatch.setattr(dev_loop, "_run_git", fake_git)
    assert dev_loop.polygon_marker(base_ref="origin/main") == ""
