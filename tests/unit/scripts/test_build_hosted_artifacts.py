"""Tests for scripts/build_hosted_artifacts.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest


def _load_builder_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import build_hosted_artifacts

    return build_hosted_artifacts


def _write_release(path: Path, *, nodes: int = 1, edges: int = 1) -> None:
    path.mkdir(parents=True)
    (path / "build_manifest.json").write_text("{}\n", encoding="utf-8")
    (path / "knockout_artifacts.json").write_text("{}\n", encoding="utf-8")
    (path / "graph_snapshot.json").write_text(
        json.dumps({"counts": {"nodes": nodes, "logic_edges": edges}}) + "\n",
        encoding="utf-8",
    )


def test_validate_release_requires_non_empty_graph(tmp_path: Path) -> None:
    builder = _load_builder_module()
    release = tmp_path / "release"
    _write_release(release, nodes=1, edges=0)

    with pytest.raises(RuntimeError, match="no nodes or no logic edges"):
        builder.validate_release(release, allow_empty_graph=False)

    builder.validate_release(release, allow_empty_graph=True)


def test_publish_current_repoints_symlink(tmp_path: Path) -> None:
    builder = _load_builder_module()
    artifact_dir = tmp_path / "artifacts"
    _write_release(artifact_dir / "releases" / "old")
    _write_release(artifact_dir / "releases" / "new")

    builder.publish_current(artifact_dir, "old")
    assert (artifact_dir / "current" / "graph_snapshot.json").is_file()

    builder.publish_current(artifact_dir, "new")
    assert (artifact_dir / "current").resolve() == (
        artifact_dir / "releases" / "new"
    ).resolve()


def test_run_forever_carries_fixture_input_parquet(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = SimpleNamespace(
        pipeline_python=Path("/usr/bin/python3"),
        artifact_dir=tmp_path / "artifacts",
        graph_repo=tmp_path / "graph",
        graph_python=None,
        graph_lookback_days=30,
        skip_refresh=True,
        skip_dbt=True,
        input_parquet=tmp_path / "fixture.parquet",
        allow_stale_current=True,
        allow_empty_graph=False,
        interval_seconds=3600,
    )

    with (
        patch.object(builder.subprocess, "run") as run,
        patch.object(builder.time, "sleep", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        builder.run_forever(args)

    command = run.call_args.args[0]
    assert "--input-parquet" in command
    assert str(args.input_parquet) in command
    assert "--refresh-command" not in command


def test_run_refresh_uses_fixed_dagster_command(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = SimpleNamespace(
        pipeline_python=Path("/usr/bin/python3"),
        skip_refresh=False,
    )

    with patch.object(builder.subprocess, "run") as run:
        builder.run_refresh(args)

    assert run.call_args.kwargs.get("shell") is not True
    assert run.call_args.kwargs["cwd"] == builder.REPO_ROOT
    command = run.call_args.args[0]
    assert command == [
        "/usr/bin/python3",
        "-m",
        "dagster",
        "job",
        "execute",
        "-m",
        "oddsfox_pipeline.orchestration.definitions",
        "-j",
        "polymarket_wc2026_full_pipeline",
    ]
