#!/usr/bin/env python3
"""Build and atomically publish a local WC2026 logical-atlas release."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
INPUT_CONTRACT = "polymarket-wc2026-logical-v1"
INPUT_BUNDLE_RELATIVE = Path("input") / INPUT_CONTRACT
GRAPH_RELATIVE = Path("graph")
RELEASE_MANIFEST_NAME = "release_manifest.json"
ROLLBACK_RECEIPT_SCHEMA = "oddsfox-legacy-rollback-receipt-v1"
ROLLBACK_RECEIPTS_RELATIVE = Path("rollback-receipts")
BROWSER_SMOKE_RECEIPTS_RELATIVE = Path("browser-smoke-receipts")
BROWSER_SMOKE_RECEIPT_SCHEMA = "wc2026-atlas-browser-smoke-v1"
PREVIOUS_LINK_NAME = "previous"
ACTIVATION_LOCK_NAME = ".activation.lock"
REQUIRED_INPUT_FILES = (
    "manifest.json",
    "events.parquet",
    "markets.parquet",
    "market_events.parquet",
    "propositions.parquet",
    "entities.parquet",
    "proposition_entities.parquet",
    "scopes.parquet",
)
REQUIRED_GRAPH_FILES = (
    "build_manifest.json",
    "viewer_manifest.json",
    "coverage_summary.json",
    "oddsfox_graph.duckdb",
    "proposition_edges.parquet",
    "market_edges.parquet",
    "market_edge_proofs.parquet",
    "constraint_groups.parquet",
    "constraint_members.parquet",
)
LEGACY_REQUIRED_FILES = (
    "build_manifest.json",
    "graph_snapshot.json",
    "knockout_artifacts.json",
    "oddsfox_graph.duckdb",
)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifact_dir = args.artifact_dir.resolve()
    releases_dir = artifact_dir / "releases"
    if args.validate_release:
        release_id = validate_release_id(args.validate_release)
        release_dir = validate_release_for_activation(
            artifact_dir,
            release_id,
            allow_empty_graph=args.allow_empty_graph,
        )
        if not is_legacy_release(release_dir):
            validate_graph_acceptance_for_release(args, release_dir)
        print(f"Validated {releases_dir / release_id}")
        return 0
    if args.activate_release:
        release_id = validate_release_id(args.activate_release)
        with activation_lock(artifact_dir):
            release_dir = validate_release_for_activation(
                artifact_dir,
                release_id,
                allow_empty_graph=args.allow_empty_graph,
            )
            if not is_legacy_release(release_dir):
                validate_graph_acceptance_for_release(args, release_dir)
                validate_browser_smoke_receipt(
                    args,
                    artifact_dir,
                    release_id,
                    release_dir,
                )
            _publish_current_locked(artifact_dir, release_id)
        print(f"Activated {release_dir}")
        print(f"Current -> {artifact_dir / 'current'}")
        return 0

    preflight_release_revisions(args)
    release_id = validate_release_id(args.release_id or utc_build_id())
    release_dir = releases_dir / release_id
    tmp_dir = releases_dir / f".{release_id}.tmp"

    if release_dir.exists() or tmp_dir.exists():
        raise SystemExit(f"release already exists: {release_id}")

    releases_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True)
    try:
        run_refresh(args)
        # The logical-atlas Dagster job already materializes its tagged dbt
        # ancestors and tests. A separate dbt build is only useful when raw
        # refresh is explicitly skipped.
        if args.skip_refresh:
            run_dbt(args)
        input_bundle = prepare_graph_input(args, tmp_dir)
        graph_dir = tmp_dir / GRAPH_RELATIVE
        build_graph(args, input_bundle, graph_dir)
        validate_graph_acceptance(args, input_bundle, graph_dir)
        write_release_manifest(args, input_bundle, graph_dir, tmp_dir)
        validate_release(tmp_dir, allow_empty_graph=args.allow_empty_graph)
        tmp_dir.rename(release_dir)
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    print(f"Built shadow release {release_dir}")
    print("Shadow release validated; current was not changed")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=Path("/artifacts"))
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=Path(os.environ.get("DUCKDB_PATH", REPO_ROOT / "oddsfox.duckdb")),
    )
    parser.add_argument(
        "--graph-repo", type=Path, default=REPO_ROOT.parent / "oddsfox-graph"
    )
    parser.add_argument("--pipeline-python", type=Path, default=Path(sys.executable))
    parser.add_argument("--graph-python", type=Path, default=None)
    parser.add_argument(
        "--pipeline-git-sha",
        default=os.environ.get("ODDSFOX_PIPELINE_GIT_SHA", ""),
    )
    parser.add_argument(
        "--graph-git-sha",
        default=os.environ.get("ODDSFOX_GRAPH_GIT_SHA", ""),
    )
    parser.add_argument("--release-id", default="")
    parser.add_argument(
        "--activate-release",
        default="",
        help="Validate and atomically activate an existing release without rebuilding",
    )
    parser.add_argument(
        "--validate-release",
        default="",
        help="Validate an existing release without changing current",
    )
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help="Deprecated compatibility flag; builds are always shadow releases",
    )
    parser.add_argument("--skip-refresh", action="store_true")
    parser.add_argument("--skip-dbt", action="store_true")
    parser.add_argument("--input-bundle", type=Path, default=None)
    parser.add_argument("--graph-mode", choices=("fast", "full"), default="fast")
    parser.add_argument("--graph-cache-dir", type=Path, default=None)
    parser.add_argument("--graph-compute-profile", type=Path, default=None)
    parser.add_argument("--graph-automation-profile", type=Path, default=None)
    parser.add_argument("--graph-primary-model-manifest", type=Path, default=None)
    parser.add_argument("--graph-verifier-model-manifest", type=Path, default=None)
    parser.add_argument("--graph-primary-base-url", default="http://127.0.0.1:8080/v1")
    parser.add_argument("--graph-verifier-base-url", default="http://127.0.0.1:8081/v1")
    parser.add_argument("--allow-empty-graph", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=0)
    args = parser.parse_args(argv)
    if args.activate_release and args.validate_release:
        parser.error("--activate-release and --validate-release are mutually exclusive")
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
            "--duckdb-path",
            str(args.duckdb_path),
            "--pipeline-python",
            str(args.pipeline_python),
            "--graph-mode",
            str(args.graph_mode),
        ]
        _append_optional_path(command, "--graph-python", args.graph_python)
        _append_optional_text(command, "--pipeline-git-sha", args.pipeline_git_sha)
        _append_optional_text(command, "--graph-git-sha", args.graph_git_sha)
        if args.skip_refresh:
            command.append("--skip-refresh")
        if args.skip_dbt:
            command.append("--skip-dbt")
        _append_optional_path(command, "--input-bundle", args.input_bundle)
        _append_full_graph_args(command, args)
        if args.allow_empty_graph:
            command.append("--allow-empty-graph")
        if args.no_publish:
            command.append("--no-publish")
        subprocess.run(command, cwd=REPO_ROOT, check=True)
        time.sleep(args.interval_seconds)


def run_refresh(args: argparse.Namespace) -> None:
    if args.skip_refresh:
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
            "polymarket_wc2026_logical_atlas",
        ],
        cwd=REPO_ROOT,
        env=pipeline_env(args),
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
            "--select",
            "+tag:wc2026_logical_atlas",
            "--exclude",
            "tag:polygon_settlement",
            "tag:pmxt_order_book",
        ],
        cwd=REPO_ROOT,
        env=pipeline_env(args),
        check=True,
    )


def prepare_graph_input(args: argparse.Namespace, release_dir: Path) -> Path:
    output_dir = release_dir / INPUT_BUNDLE_RELATIVE
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if args.input_bundle:
        source = args.input_bundle.resolve()
        if not source.is_dir():
            raise ValueError(f"input bundle is not a directory: {source}")
        shutil.copytree(source, output_dir)
    else:
        subprocess.run(
            [
                str(args.pipeline_python),
                "scripts/export_polymarket_wc2026_logical_bundle.py",
                "--duckdb-path",
                str(args.duckdb_path),
                "--output-dir",
                str(output_dir),
            ],
            cwd=REPO_ROOT,
            check=True,
        )
    _require_files(output_dir, REQUIRED_INPUT_FILES, "input bundle")
    return output_dir


def build_graph(args: argparse.Namespace, input_bundle: Path, graph_dir: Path) -> None:
    graph_python = args.graph_python or default_graph_python(args.graph_repo)
    command = [
        str(graph_python),
        "-m",
        "oddsfox_graph.cli",
        "discover",
        "--mode",
        str(args.graph_mode),
        "--input",
        str(input_bundle),
        "--input-profile",
        INPUT_CONTRACT,
        "--out",
        str(graph_dir),
    ]
    _append_full_graph_args(command, args)
    env = dict(os.environ)
    env["PYTHONPATH"] = prepend_pythonpath(args.graph_repo, env.get("PYTHONPATH", ""))
    subprocess.run(command, cwd=args.graph_repo, env=env, check=True)


def validate_graph_acceptance(
    args: argparse.Namespace,
    input_bundle: Path,
    graph_dir: Path,
) -> dict[str, object]:
    """Run the Graph-owned deterministic acceptance suite for one candidate."""
    graph_python = args.graph_python or default_graph_python(args.graph_repo)
    command = [
        str(graph_python),
        "-m",
        "oddsfox_graph.cli",
        "atlas-validate",
        "--bundle-dir",
        str(input_bundle),
        "--graph-dir",
        str(graph_dir),
        "--output-format",
        "json",
    ]
    env = dict(os.environ)
    env["PYTHONPATH"] = prepend_pythonpath(args.graph_repo, env.get("PYTHONPATH", ""))
    completed = subprocess.run(
        command,
        cwd=args.graph_repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        report = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise RuntimeError("Graph atlas acceptance returned invalid JSON") from exc
    if not isinstance(report, dict) or report.get("schema_version") != (
        "wc2026-atlas-acceptance-v1"
    ):
        raise RuntimeError("Graph atlas acceptance returned an unknown schema")
    if report.get("passed") is not True:
        raise RuntimeError("Graph atlas acceptance did not report passed=true")
    return report


def validate_graph_acceptance_for_release(
    args: argparse.Namespace,
    release_dir: Path,
) -> dict[str, object] | None:
    """Re-run logical-atlas acceptance before validating or activating a release."""
    input_bundle = release_dir / INPUT_BUNDLE_RELATIVE
    graph_dir = release_dir / GRAPH_RELATIVE
    if not input_bundle.is_dir():
        return None
    return validate_graph_acceptance(args, input_bundle, graph_dir)


def browser_smoke_receipt_path(artifact_dir: Path, release_id: str) -> Path:
    """Return the operator-visible receipt path for one immutable release."""
    return (
        artifact_dir.resolve()
        / BROWSER_SMOKE_RECEIPTS_RELATIVE
        / f"{validate_release_id(release_id)}.json"
    )


def validate_browser_smoke_receipt(
    args: argparse.Namespace,
    artifact_dir: Path,
    release_id: str,
    release_dir: Path,
) -> dict[str, object] | None:
    """Ask Graph to verify the manifest-bound browser receipt before cutover."""
    graph_dir = release_dir / GRAPH_RELATIVE
    if not (release_dir / INPUT_BUNDLE_RELATIVE).is_dir():
        return None
    receipt = browser_smoke_receipt_path(artifact_dir, release_id)
    if not receipt.is_file():
        raise RuntimeError(
            f"Logical-atlas activation requires a browser-smoke receipt: {receipt}"
        )
    graph_python = args.graph_python or default_graph_python(args.graph_repo)
    command = [
        str(graph_python),
        "-m",
        "oddsfox_graph.cli",
        "atlas-browser-receipt-validate",
        "--graph-dir",
        str(graph_dir),
        "--receipt",
        str(receipt),
        "--output-format",
        "json",
    ]
    env = dict(os.environ)
    env["PYTHONPATH"] = prepend_pythonpath(args.graph_repo, env.get("PYTHONPATH", ""))
    completed = subprocess.run(
        command,
        cwd=args.graph_repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        report = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise RuntimeError(
            "Graph browser-smoke receipt validator returned invalid JSON"
        ) from exc
    if not isinstance(report, dict) or report.get("schema_version") != (
        BROWSER_SMOKE_RECEIPT_SCHEMA
    ):
        raise RuntimeError("Graph browser-smoke receipt has an unknown schema")
    if report.get("passed") is not True or report.get("validated") is not True:
        raise RuntimeError("Graph browser-smoke receipt was not validated")
    return report


def write_release_manifest(
    args: argparse.Namespace,
    input_bundle: Path,
    graph_dir: Path,
    release_dir: Path,
) -> None:
    pipeline_sha = release_git_sha(
        REPO_ROOT,
        args.pipeline_git_sha,
        label="Pipeline",
    )
    graph_sha = release_git_sha(
        args.graph_repo,
        args.graph_git_sha,
        label="Graph",
    )
    input_manifest_path = input_bundle / "manifest.json"
    graph_manifest_path = graph_dir / "build_manifest.json"
    input_manifest = load_json_object(input_manifest_path, label="input manifest")
    graph_manifest = load_json_object(graph_manifest_path, label="graph manifest")
    input_manifest_sha256 = sha256_file(input_manifest_path)
    graph_manifest_sha256 = sha256_file(graph_manifest_path)
    validate_inner_manifest_bindings(
        input_manifest=input_manifest,
        graph_manifest=graph_manifest,
        pipeline_sha=pipeline_sha,
        graph_sha=graph_sha,
        graph_mode=args.graph_mode,
        input_manifest_sha256=input_manifest_sha256,
    )
    payload = {
        "schema": "oddsfox-local-atlas-release-v1",
        "built_at": datetime.now(timezone.utc).isoformat(),
        "input_contract": INPUT_CONTRACT,
        "pipeline_git_sha": pipeline_sha,
        "graph_git_sha": graph_sha,
        "graph_mode": args.graph_mode,
        "temporal_odds": False,
        "input_manifest_sha256": input_manifest_sha256,
        "graph_manifest_sha256": graph_manifest_sha256,
        "input_files": file_hashes(input_bundle),
        "graph_files": file_hashes(graph_dir),
    }
    target = release_dir / RELEASE_MANIFEST_NAME
    target.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def validate_release(release_dir: Path, *, allow_empty_graph: bool) -> None:
    input_bundle = release_dir / INPUT_BUNDLE_RELATIVE
    graph_dir = release_dir / GRAPH_RELATIVE
    _require_files(input_bundle, REQUIRED_INPUT_FILES, "input bundle")
    _require_files(graph_dir, REQUIRED_GRAPH_FILES, "graph output")
    release_manifest_path = release_dir / RELEASE_MANIFEST_NAME
    if not release_manifest_path.is_file():
        raise RuntimeError(f"missing required release file: {RELEASE_MANIFEST_NAME}")

    release_manifest = load_json_object(
        release_manifest_path,
        label="release manifest",
    )
    if release_manifest.get("schema") != "oddsfox-local-atlas-release-v1":
        raise RuntimeError("release manifest has an unsupported schema")
    if release_manifest.get("input_contract") != INPUT_CONTRACT:
        raise RuntimeError("release manifest has an unsupported input contract")
    graph_mode = release_manifest.get("graph_mode")
    if graph_mode not in {"fast", "full"}:
        raise RuntimeError("release manifest has an invalid graph mode")
    if release_manifest.get("temporal_odds") is not False:
        raise RuntimeError("release manifest must declare temporal_odds=false")
    for field in ("pipeline_git_sha", "graph_git_sha"):
        value = release_manifest.get(field)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value) is None:
            raise RuntimeError(f"release manifest has an invalid {field}")
    input_manifest_hash = sha256_file(input_bundle / "manifest.json")
    graph_manifest_hash = sha256_file(graph_dir / "build_manifest.json")
    if release_manifest.get("input_manifest_sha256") != input_manifest_hash:
        raise RuntimeError("release input manifest hash binding does not match")
    if release_manifest.get("graph_manifest_sha256") != graph_manifest_hash:
        raise RuntimeError("release graph manifest hash binding does not match")
    input_manifest = load_json_object(
        input_bundle / "manifest.json",
        label="input manifest",
    )
    graph_manifest = load_json_object(
        graph_dir / "build_manifest.json",
        label="graph manifest",
    )
    validate_inner_manifest_bindings(
        input_manifest=input_manifest,
        graph_manifest=graph_manifest,
        pipeline_sha=str(release_manifest["pipeline_git_sha"]),
        graph_sha=str(release_manifest["graph_git_sha"]),
        graph_mode=str(graph_mode),
        input_manifest_sha256=input_manifest_hash,
    )
    validate_file_hashes(
        input_bundle,
        release_manifest.get("input_files"),
        label="input bundle",
    )
    validate_file_hashes(
        graph_dir,
        release_manifest.get("graph_files"),
        label="graph output",
    )

    if allow_empty_graph:
        return
    with duckdb.connect(
        str(graph_dir / "oddsfox_graph.duckdb"), read_only=True
    ) as connection:
        event_count = int(
            connection.execute("SELECT count(*) FROM events").fetchone()[0]
        )
        market_count = int(
            connection.execute("SELECT count(*) FROM markets").fetchone()[0]
        )
        visible_market_count = int(
            connection.execute("SELECT count(*) FROM market_summary_v").fetchone()[0]
        )
        membership_market_count = int(
            connection.execute(
                "SELECT count(DISTINCT market_id) FROM market_events"
            ).fetchone()[0]
        )
        edge_count = int(
            connection.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [str(graph_dir / "market_edges.parquet")],
            ).fetchone()[0]
        )
    if event_count == 0 or market_count == 0 or edge_count == 0:
        raise RuntimeError("graph artifact has no events, markets, or logical edges")
    if visible_market_count != market_count or membership_market_count != market_count:
        raise RuntimeError(
            "eligible child-market inventory does not match visible market nodes: "
            f"markets={market_count}, visible={visible_market_count}, "
            f"memberships={membership_market_count}"
        )


def validate_release_for_activation(
    artifact_dir: Path,
    release_id: str,
    *,
    allow_empty_graph: bool,
) -> Path:
    release_id = validate_release_id(release_id)
    release_dir = artifact_dir.resolve() / "releases" / release_id
    if not release_dir.is_dir():
        raise SystemExit(f"release does not exist: {release_id}")
    if (release_dir / RELEASE_MANIFEST_NAME).is_file():
        validate_release(release_dir, allow_empty_graph=allow_empty_graph)
    else:
        validate_legacy_release(artifact_dir.resolve(), release_id)
    return release_dir


def is_legacy_release(release_dir: Path) -> bool:
    """Return whether a validated release predates the logical-atlas format."""
    return not (release_dir / RELEASE_MANIFEST_NAME).is_file()


def validate_legacy_release(artifact_dir: Path, release_id: str) -> None:
    release_id = validate_release_id(release_id)
    release_dir = artifact_dir / "releases" / release_id
    _require_files(release_dir, LEGACY_REQUIRED_FILES, "legacy release")
    receipt_path = rollback_receipt_path(artifact_dir, release_id)
    if not receipt_path.is_file():
        raise RuntimeError(
            f"legacy release has no sealed rollback receipt: {release_id}"
        )
    receipt = load_json_object(receipt_path, label="legacy rollback receipt")
    if receipt.get("schema") != ROLLBACK_RECEIPT_SCHEMA:
        raise RuntimeError("legacy rollback receipt has an unsupported schema")
    if receipt.get("release_id") != release_id:
        raise RuntimeError("legacy rollback receipt release ID does not match")
    if receipt.get("pipeline_git_sha") is not None:
        validate_git_sha(str(receipt["pipeline_git_sha"]), label="legacy Pipeline")
    if receipt.get("graph_git_sha") is not None:
        validate_git_sha(str(receipt["graph_git_sha"]), label="legacy Graph")
    validate_file_hashes(
        release_dir,
        receipt.get("files"),
        label="legacy release",
    )


def seal_legacy_release(artifact_dir: Path, release_id: str) -> Path:
    """Content-seal an immutable pre-atlas release without inventing code SHAs."""
    release_id = validate_release_id(release_id)
    release_dir = artifact_dir / "releases" / release_id
    _require_files(release_dir, LEGACY_REQUIRED_FILES, "legacy release")
    receipt_path = rollback_receipt_path(artifact_dir, release_id)
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    if receipt_path.exists():
        validate_legacy_release(artifact_dir, release_id)
        return receipt_path
    payload = {
        "schema": ROLLBACK_RECEIPT_SCHEMA,
        "sealed_at": datetime.now(timezone.utc).isoformat(),
        "release_id": release_id,
        "pipeline_git_sha": None,
        "graph_git_sha": None,
        "revision_note": (
            "Historical code SHAs were not recorded by the legacy release format; "
            "artifact bytes are sealed without fabricating provenance."
        ),
        "files": file_hashes(release_dir),
    }
    temporary = receipt_path.with_name(f".{receipt_path.name}.{uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, receipt_path)
    validate_legacy_release(artifact_dir, release_id)
    return receipt_path


def rollback_receipt_path(artifact_dir: Path, release_id: str) -> Path:
    return artifact_dir / ROLLBACK_RECEIPTS_RELATIVE / f"{release_id}.json"


@contextmanager
def activation_lock(artifact_dir: Path) -> Iterator[None]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    lock_path = artifact_dir / ACTIVATION_LOCK_NAME
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def publish_current(artifact_dir: Path, release_id: str) -> None:
    release_id = validate_release_id(release_id)
    artifact_dir = artifact_dir.resolve()
    with activation_lock(artifact_dir):
        _publish_current_locked(artifact_dir, release_id)


def _publish_current_locked(artifact_dir: Path, release_id: str) -> None:
    """Replace current while the caller holds ``activation_lock``."""
    release_id = validate_release_id(release_id)
    artifact_dir = artifact_dir.resolve()
    current = artifact_dir / "current"
    previous_release_id = current_release_id(artifact_dir)
    if previous_release_id is not None and previous_release_id != release_id:
        previous_dir = artifact_dir / "releases" / previous_release_id
        if not (previous_dir / RELEASE_MANIFEST_NAME).is_file():
            seal_legacy_release(artifact_dir, previous_release_id)
        replace_release_symlink(
            artifact_dir,
            PREVIOUS_LINK_NAME,
            previous_release_id,
        )
    replace_release_symlink(artifact_dir, current.name, release_id)


def current_release_id(artifact_dir: Path) -> str | None:
    current = artifact_dir / "current"
    if not current.is_symlink():
        if current.exists():
            raise RuntimeError("current must be a symlink into releases")
        return None
    target = os.readlink(current)
    match = re.fullmatch(r"releases/([A-Za-z0-9][A-Za-z0-9._-]*)", target)
    if match is None:
        raise RuntimeError(f"current has an unsafe release target: {target!r}")
    return validate_release_id(match.group(1))


def replace_release_symlink(artifact_dir: Path, name: str, release_id: str) -> None:
    tmp_link = artifact_dir / f".{name}.{os.getpid()}.{uuid4().hex}.tmp"
    target = Path("releases") / validate_release_id(release_id)
    try:
        os.symlink(target, tmp_link, target_is_directory=True)
        os.replace(tmp_link, artifact_dir / name)
    finally:
        if tmp_link.exists() or tmp_link.is_symlink():
            tmp_link.unlink()


def validate_release_id(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value) is None:
        raise ValueError(f"invalid release ID: {value!r}")
    return value


def default_graph_python(graph_repo: Path) -> Path:
    venv_python = graph_repo / ".venv" / "bin" / "python"
    return venv_python if venv_python.exists() else Path(sys.executable)


def prepend_pythonpath(path: Path, existing: str) -> str:
    return str(path) if not existing else f"{path}{os.pathsep}{existing}"


def pipeline_env(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    env["DUCKDB_PATH"] = str(args.duckdb_path.resolve())
    return env


def preflight_release_revisions(args: argparse.Namespace) -> None:
    """Fail before refresh when either checkout cannot bind a clean revision."""
    args.pipeline_git_sha = release_git_sha(
        REPO_ROOT,
        args.pipeline_git_sha,
        label="Pipeline",
    )
    args.graph_git_sha = release_git_sha(
        args.graph_repo,
        args.graph_git_sha,
        label="Graph",
    )


def release_git_sha(repo: Path, explicit: str, *, label: str) -> str:
    """Resolve one trustworthy release revision.

    A local checkout must be clean and an explicit revision must equal its HEAD.
    Container builds omit ``.git`` and therefore have to inject the exact SHA.
    """
    repo = repo.resolve()
    has_git_metadata = (repo / ".git").exists()
    if explicit:
        validate_git_sha(explicit, label=label)
        if not has_git_metadata:
            return explicit
        observed = git_sha(repo)
        if observed != explicit:
            raise RuntimeError(
                f"{label} Git SHA does not match checkout HEAD: "
                f"expected={explicit}, observed={observed}"
            )
    else:
        if not has_git_metadata:
            raise RuntimeError(
                f"{label} Git SHA must be injected when Git metadata is unavailable"
            )
        observed = git_sha(repo)
        explicit = observed
    status = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise RuntimeError(f"{label} worktree must be clean for a release build")
    return explicit


def git_sha(repo: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    value = completed.stdout.strip()
    if len(value) != 40:
        raise RuntimeError(f"invalid Git SHA for {repo}: {value!r}")
    return value


def validate_git_sha(value: str, *, label: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise RuntimeError(f"invalid {label} Git SHA: {value!r}")


def load_json_object(path: Path, *, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{label} must contain a JSON object: {path}")
    return payload


def validate_inner_manifest_bindings(
    *,
    input_manifest: dict[str, object],
    graph_manifest: dict[str, object],
    pipeline_sha: str,
    graph_sha: str,
    graph_mode: str,
    input_manifest_sha256: str,
) -> None:
    if input_manifest.get("schema_version") != INPUT_CONTRACT:
        raise RuntimeError("input manifest schema_version does not match release")
    if input_manifest.get("pipeline_git_sha") != pipeline_sha:
        raise RuntimeError("input manifest Pipeline SHA does not match release")
    if input_manifest.get("temporal_odds") is not False:
        raise RuntimeError("input manifest must declare temporal_odds=false")
    if graph_manifest.get("build_mode") != graph_mode:
        raise RuntimeError("graph manifest build mode does not match release")
    if graph_manifest.get("graph_git_sha") != graph_sha:
        raise RuntimeError("graph manifest Graph SHA does not match release")
    if graph_manifest.get("graph_worktree_dirty") is not False:
        raise RuntimeError("graph manifest must attest to a clean worktree")
    graph_input = graph_manifest.get("input")
    if not isinstance(graph_input, dict):
        raise RuntimeError("graph manifest is missing its input binding")
    if graph_input.get("profile") != INPUT_CONTRACT:
        raise RuntimeError("graph manifest input profile does not match release")
    if graph_input.get("schema") != INPUT_CONTRACT:
        raise RuntimeError("graph manifest input schema does not match release")
    if graph_input.get("manifest_sha256") != input_manifest_sha256:
        raise RuntimeError("graph manifest input hash does not match input manifest")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_hashes(root: Path) -> dict[str, str]:
    """Return a deterministic, recursive inventory for one release subtree."""
    return {
        path.relative_to(root).as_posix(): sha256_file(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def validate_file_hashes(root: Path, expected: object, *, label: str) -> None:
    if not isinstance(expected, dict) or not expected:
        raise RuntimeError(f"release manifest has no {label} hash inventory")
    if not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in expected.items()
    ):
        raise RuntimeError(f"release manifest has an invalid {label} hash inventory")
    actual = file_hashes(root)
    if actual != expected:
        missing = sorted(set(expected) - set(actual))
        unexpected = sorted(set(actual) - set(expected))
        changed = sorted(
            key for key in set(actual) & set(expected) if actual[key] != expected[key]
        )
        raise RuntimeError(
            f"{label} hash inventory mismatch: missing={missing}, "
            f"unexpected={unexpected}, changed={changed}"
        )


def _require_files(root: Path, names: tuple[str, ...], label: str) -> None:
    missing = [name for name in names if not (root / name).is_file()]
    if missing:
        raise RuntimeError(f"{label} is missing required files: {', '.join(missing)}")


def _append_optional_path(command: list[str], option: str, value: Path | None) -> None:
    if value is not None:
        command.extend([option, str(value)])


def _append_optional_text(command: list[str], option: str, value: str) -> None:
    if value:
        command.extend([option, value])


def _append_full_graph_args(command: list[str], args: argparse.Namespace) -> None:
    if args.graph_mode != "full":
        return
    for option, value in (
        ("--cache-dir", args.graph_cache_dir),
        ("--compute-profile", args.graph_compute_profile),
        ("--automation-profile", args.graph_automation_profile),
        ("--primary-model-manifest", args.graph_primary_model_manifest),
        ("--verifier-model-manifest", args.graph_verifier_model_manifest),
    ):
        _append_optional_path(command, option, value)
    command.extend(
        [
            "--primary-base-url",
            args.graph_primary_base_url,
            "--verifier-base-url",
            args.graph_verifier_base_url,
        ]
    )


def utc_build_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


if __name__ == "__main__":
    raise SystemExit(main())
