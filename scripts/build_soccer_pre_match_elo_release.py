#!/usr/bin/env python3
"""Acquire, inspect, or publish the event-grain soccer pre-match Elo release."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT = ensure_src_on_path()

from oddsfox_pipeline.features.pre_match_elo.identity_authoring import (  # noqa: E402
    audit_identity_map,
    compile_identity_map,
    prepare_identity_review,
    review_identity_candidates,
)
from oddsfox_pipeline.features.pre_match_elo.release import (  # noqa: E402
    DATASET_VERSION,
    TARGET_EVENT_COUNT,
    TARGET_SNAPSHOT_SHA256,
    build_release,
    load_target_events,
    normalize_sources,
)
from oddsfox_pipeline.features.pre_match_elo.sources import (  # noqa: E402
    acquire_snapshots,
    load_source_catalog,
)
from oddsfox_pipeline.publishing._bundle_io import (  # noqa: E402
    current_clean_commit,
    sha256_file,
    write_json,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("acquire", "inspect"):
        command = subparsers.add_parser(name)
        command.add_argument("--source-catalog", type=Path, required=True)
        command.add_argument("--raw-root", type=Path, required=True)
    inspect = subparsers.choices["inspect"]
    inspect.add_argument("--output-directory", type=Path, required=True)

    prepare = subparsers.add_parser("prepare-identities")
    prepare.add_argument("--target-parquet", type=Path, required=True)
    prepare.add_argument("--source-catalog", type=Path, required=True)
    prepare.add_argument("--raw-root", type=Path, required=True)
    prepare.add_argument("--output-directory", type=Path, required=True)

    review = subparsers.add_parser("review-identities")
    review.add_argument("--workspace", type=Path, required=True)
    review.add_argument("--reviewer", required=True)

    compile_command = subparsers.add_parser("compile-identities")
    compile_command.add_argument("--workspace", type=Path, required=True)
    compile_command.add_argument("--identity-map", type=Path, required=True)
    compile_command.add_argument("--review-report", type=Path, required=True)

    audit = subparsers.add_parser("audit-identities")
    audit.add_argument("--target-parquet", type=Path, required=True)
    audit.add_argument("--source-catalog", type=Path, required=True)
    audit.add_argument("--raw-root", type=Path, required=True)
    audit.add_argument("--identity-map", type=Path, required=True)
    audit.add_argument("--identity-review-report", type=Path, required=True)
    audit.add_argument("--benchmark-path", type=Path)
    audit.add_argument("--output-directory", type=Path, required=True)

    build = subparsers.add_parser("build")
    build.add_argument("--target-parquet", type=Path, required=True)
    build.add_argument("--source-catalog", type=Path, required=True)
    build.add_argument("--raw-root", type=Path, required=True)
    build.add_argument("--identity-map", type=Path, required=True)
    build.add_argument("--identity-review-report", type=Path, required=True)
    build.add_argument("--benchmark-path", type=Path)
    build.add_argument("--dataset-version", default=DATASET_VERSION)
    build.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "artifacts" / "strategy-inputs" / "soccer_pre_match_elo",
    )
    return parser


def _inspect(args: argparse.Namespace) -> int:
    snapshots = load_source_catalog(args.source_catalog)
    rows, manifest, issues = normalize_sources(snapshots, args.raw_root)
    output = args.output_directory
    output.mkdir(parents=True, exist_ok=True)
    result_schema = pa.schema(
        [
            ("source_match_id", pa.string()),
            ("match_date", pa.date32()),
            ("home_name", pa.string()),
            ("away_name", pa.string()),
            ("home_score", pa.int64()),
            ("away_score", pa.int64()),
            ("competition", pa.string()),
            ("rating_pool", pa.string()),
            ("neutral", pa.bool_()),
            ("friendly", pa.bool_()),
            ("source", pa.string()),
            ("snapshot_id", pa.string()),
            ("source_locator", pa.string()),
        ]
    )
    issue_schema = pa.schema(
        [
            ("source_locator", pa.string()),
            ("reason", pa.string()),
            ("text", pa.string()),
        ]
    )
    pq.write_table(
        pa.Table.from_pylist([asdict(row) for row in rows], schema=result_schema),
        output / "normalized_results.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([asdict(issue) for issue in issues], schema=issue_schema),
        output / "parse_issues.parquet",
    )
    write_json(output / "source_manifest.json", {"snapshots": manifest})
    print(
        json.dumps(
            {
                "normalized_results": len(rows),
                "parse_issues": len(issues),
                "output_directory": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 1 if issues else 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "review-identities":
        review_identity_candidates(args.workspace, reviewer=args.reviewer)
        print(json.dumps({"workspace": str(args.workspace.resolve())}, sort_keys=True))
        return 0
    if args.command == "compile-identities":
        path = compile_identity_map(
            args.workspace,
            output_path=args.identity_map,
            report_path=args.review_report,
        )
        print(json.dumps({"identity_map": str(path.resolve())}, sort_keys=True))
        return 0
    snapshots = load_source_catalog(args.source_catalog)
    if args.command == "acquire":
        paths = acquire_snapshots(snapshots, args.raw_root)
        print(
            json.dumps(
                {"snapshots": len(paths), "raw_root": str(args.raw_root.resolve())},
                sort_keys=True,
            )
        )
        return 0
    if args.command == "inspect":
        return _inspect(args)
    if args.command == "prepare-identities":
        target_sha = sha256_file(args.target_parquet)
        if target_sha != TARGET_SNAPSHOT_SHA256:
            raise ValueError("target snapshot SHA mismatch")
        events = load_target_events(args.target_parquet)
        if len(events) != TARGET_EVENT_COUNT:
            raise ValueError("target event count mismatch")
        rows, _manifest, issues = normalize_sources(snapshots, args.raw_root)
        if issues:
            raise ValueError("identity preparation requires zero parse issues")
        workspace = prepare_identity_review(
            events=events,
            results=rows,
            target_sha256=target_sha,
            source_catalog_sha256=sha256_file(args.source_catalog),
            output_directory=args.output_directory,
        )
        print(json.dumps({"workspace": str(workspace.resolve())}, sort_keys=True))
        return 0
    if args.command == "audit-identities":
        audit = audit_identity_map(
            target_parquet=args.target_parquet,
            snapshots=snapshots,
            raw_root=args.raw_root,
            identity_map=args.identity_map,
            identity_review_report=args.identity_review_report,
            source_catalog_sha256=sha256_file(args.source_catalog),
            benchmark_path=args.benchmark_path,
            output_directory=args.output_directory,
        )
        print(json.dumps({"audit": str(audit.resolve())}, sort_keys=True))
        return 0
    build_revision = current_clean_commit(REPO_ROOT)
    destination = args.output_root / "releases" / args.dataset_version
    release = build_release(
        target_parquet=args.target_parquet,
        snapshots=snapshots,
        raw_root=args.raw_root,
        identity_map=args.identity_map,
        identity_review_report=args.identity_review_report,
        source_catalog_sha256=sha256_file(args.source_catalog),
        benchmark_path=args.benchmark_path,
        output_directory=destination,
        build_revision=build_revision,
        dataset_version=args.dataset_version,
    )
    print(json.dumps({"release": str(release.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
