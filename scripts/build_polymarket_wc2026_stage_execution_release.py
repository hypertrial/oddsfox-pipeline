#!/usr/bin/env python3
"""Plan or build the targeted WC2026 stage execution-evidence release."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

from oddsfox_pipeline.config.settings_warehouse import DUCKDB_PATH  # noqa: E402
from oddsfox_pipeline.publishing.stage_execution import (  # noqa: E402
    DATASET_VERSION,
    DEFAULT_OUTPUT_ROOT,
    acquire_execution_evidence,
    build_execution_plan,
    preflight_execution_release,
    publish_execution_release,
)
from oddsfox_pipeline.publishing.stage_execution_archive import (  # noqa: E402
    acquire_archive_execution_evidence,
    archive_plan_summary,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("mode", choices=("plan", "release"))
    value.add_argument("--stage-minute-release", type=Path, required=True)
    value.add_argument("--ohlc-report", type=Path, required=True)
    value.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    value.add_argument("--state-path", type=Path)
    value.add_argument("--credit-ledger-path", type=Path, default=DUCKDB_PATH)
    value.add_argument("--request-budget", type=int, default=20_000)
    value.add_argument("--dataset-version", default=DATASET_VERSION)
    value.add_argument(
        "--source", choices=("archive-v2", "api-range"), default="archive-v2"
    )
    return value


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        plan = build_execution_plan(
            args.stage_minute_release,
            args.ohlc_report,
            request_budget=args.request_budget,
        )
        summary = (
            archive_plan_summary(plan)
            if args.source == "archive-v2"
            else plan.summary()
        )
        if args.mode == "plan":
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 0 if summary["within_budget"] else 2
        _, generator_commit = preflight_execution_release(
            args.output_root, dataset_version=args.dataset_version
        )
        state_path = args.state_path or (
            args.output_root / ".state" / f"{args.dataset_version}.duckdb"
        )
        if args.source == "archive-v2":
            connection = acquire_archive_execution_evidence(
                plan, state_path, credit_ledger_path=args.credit_ledger_path
            )
        else:
            connection = acquire_execution_evidence(
                plan, state_path, credit_ledger_path=args.credit_ledger_path
            )
        try:
            release = publish_execution_release(
                plan,
                connection,
                args.output_root,
                generator_commit=generator_commit,
                dataset_version=args.dataset_version,
            )
        finally:
            connection.close()
        print(
            json.dumps(
                {**summary, "release_dir": str(release)}, indent=2, sort_keys=True
            )
        )
        return 0
    except (
        OSError,
        RuntimeError,
        ValueError,
        duckdb.Error,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
