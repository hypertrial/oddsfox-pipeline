#!/usr/bin/env python3
"""Build and publish hosted WC2026 graph artifacts."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GRAPH_INPUT_NAME = "polymarket_wc2026_graph_token_hourly_odds.parquet"
GRAPH_JSON_NAME = "graph_snapshot.json"
KNOCKOUT_JSON_NAME = "knockout_artifacts.json"
MANIFEST_NAME = "build_manifest.json"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifact_dir = args.artifact_dir.resolve()
    releases_dir = artifact_dir / "releases"
    release_id = args.release_id or utc_build_id()
    release_dir = releases_dir / release_id
    tmp_dir = releases_dir / f".{release_id}.tmp"

    if release_dir.exists() or tmp_dir.exists():
        raise SystemExit(f"release already exists: {release_id}")

    releases_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True)
    try:
        run_refresh(args)
        run_dbt(args)
        input_path = prepare_graph_input(args, tmp_dir)
        build_graph(args, input_path, tmp_dir)
        validate_release(tmp_dir, allow_empty_graph=args.allow_empty_graph)
        tmp_dir.rename(release_dir)
        publish_current(artifact_dir, release_id)
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    print(f"Published {release_dir}")
    print(f"Current -> {artifact_dir / 'current'}")
    return 0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=Path("/artifacts"))
    parser.add_argument(
        "--graph-repo", type=Path, default=REPO_ROOT.parent / "oddsfox-graph"
    )
    parser.add_argument("--pipeline-python", type=Path, default=Path(sys.executable))
    parser.add_argument("--graph-python", type=Path, default=None)
    parser.add_argument("--release-id", default="")
    parser.add_argument("--refresh-command", default="")
    parser.add_argument("--skip-refresh", action="store_true")
    parser.add_argument("--skip-dbt", action="store_true")
    parser.add_argument("--input-parquet", type=Path, default=None)
    parser.add_argument("--graph-lookback-days", type=int, default=30)
    parser.add_argument("--allow-stale-current", action="store_true")
    parser.add_argument("--allow-empty-graph", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=0)
    args = parser.parse_args(argv)
    if args.interval_seconds > 0:
        run_forever(args)
        raise SystemExit(0)
    return args


def run_forever(args: argparse.Namespace) -> None:
    while True:
        command = [
            str(args.pipeline_python),
            str(Path(__file__).resolve()),
            "--artifact-dir",
            str(args.artifact_dir),
            "--graph-repo",
            str(args.graph_repo),
            "--pipeline-python",
            str(args.pipeline_python),
            "--graph-lookback-days",
            str(args.graph_lookback_days),
        ]
        if args.graph_python:
            command.extend(["--graph-python", str(args.graph_python)])
        if args.refresh_command:
            command.extend(["--refresh-command", args.refresh_command])
        if args.skip_refresh:
            command.append("--skip-refresh")
        if args.skip_dbt:
            command.append("--skip-dbt")
        if args.input_parquet:
            command.extend(["--input-parquet", str(args.input_parquet)])
        if args.allow_stale_current:
            command.append("--allow-stale-current")
        if args.allow_empty_graph:
            command.append("--allow-empty-graph")
        subprocess.run(command, cwd=REPO_ROOT, check=True)
        time.sleep(args.interval_seconds)


def run_refresh(args: argparse.Namespace) -> None:
    if args.skip_refresh:
        return
    if args.refresh_command:
        subprocess.run(args.refresh_command, cwd=REPO_ROOT, shell=True, check=True)
        return
    subprocess.run(
        [
            str(args.pipeline_python),
            "-m",
            "dagster",
            "job",
            "execute",
            "-m",
            "oddsfox_pipeline.orchestration.definitions",
            "-j",
            "polymarket_wc2026_full_pipeline",
        ],
        cwd=REPO_ROOT,
        check=True,
    )


def run_dbt(args: argparse.Namespace) -> None:
    if args.skip_dbt:
        return
    subprocess.run(
        [
            str(args.pipeline_python),
            "-m",
            "dbt.cli.main",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt/profiles",
        ],
        cwd=REPO_ROOT,
        check=True,
    )


def prepare_graph_input(args: argparse.Namespace, release_dir: Path) -> Path:
    output_path = release_dir / GRAPH_INPUT_NAME
    if args.input_parquet:
        shutil.copy2(args.input_parquet, output_path)
        return output_path
    subprocess.run(
        [
            str(args.pipeline_python),
            "scripts/export_polymarket_wc2026_graph_hourly_odds.py",
            "--snapshot-copy",
            "--output",
            str(output_path),
        ],
        cwd=REPO_ROOT,
        check=True,
    )
    return output_path


def build_graph(args: argparse.Namespace, input_path: Path, release_dir: Path) -> None:
    graph_python = args.graph_python or default_graph_python(args.graph_repo)
    command = [
        str(graph_python),
        "-m",
        "oddsfox_graph.cli",
        "build",
        "--input",
        str(input_path),
        "--out",
        str(release_dir),
        "--fast-graph",
        "--graph-lookback-days",
        str(args.graph_lookback_days),
    ]
    if args.allow_stale_current:
        command.append("--allow-stale-current")
    env = dict(os.environ)
    env["PYTHONPATH"] = prepend_pythonpath(args.graph_repo, env.get("PYTHONPATH", ""))
    subprocess.run(command, cwd=args.graph_repo, env=env, check=True)


def validate_release(release_dir: Path, *, allow_empty_graph: bool) -> None:
    for name in (MANIFEST_NAME, GRAPH_JSON_NAME, KNOCKOUT_JSON_NAME):
        if not (release_dir / name).is_file():
            raise RuntimeError(f"missing required artifact: {name}")
    graph = json.loads((release_dir / GRAPH_JSON_NAME).read_text(encoding="utf-8"))
    counts = graph.get("counts") or {}
    if not allow_empty_graph and (
        int(counts.get("nodes") or 0) == 0 or int(counts.get("logic_edges") or 0) == 0
    ):
        raise RuntimeError("graph artifact has no nodes or no logic edges")


def publish_current(artifact_dir: Path, release_id: str) -> None:
    tmp_link = artifact_dir / ".current.tmp"
    current = artifact_dir / "current"
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    os.symlink(Path("releases") / release_id, tmp_link, target_is_directory=True)
    os.replace(tmp_link, current)


def default_graph_python(graph_repo: Path) -> Path:
    venv_python = graph_repo / ".venv" / "bin" / "python"
    return venv_python if venv_python.exists() else Path(sys.executable)


def prepend_pythonpath(path: Path, existing: str) -> str:
    return str(path) if not existing else f"{path}{os.pathsep}{existing}"


def utc_build_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


if __name__ == "__main__":
    raise SystemExit(main())
