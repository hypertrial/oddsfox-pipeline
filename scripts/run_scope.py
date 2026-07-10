#!/usr/bin/env python3
"""Run one Dagster step for one or more shipped OddsFox scopes."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT = ensure_src_on_path()

from oddsfox_pipeline.orchestration.scope_registry import (  # noqa: E402
    SCOPE_STEPS,
    ScopeSpec,
    ScopeStep,
    get_scope_spec,
    iter_scope_specs,
)

DAGSTER_MODULE = "oddsfox_pipeline.orchestration.definitions"


def dagster_command(*, python: Path, spec: ScopeSpec, step: ScopeStep) -> list[str]:
    return [
        str(python),
        "-m",
        "dagster",
        "job",
        "execute",
        "-m",
        DAGSTER_MODULE,
        "-j",
        spec.job_for_step(step),
    ]


def _print_scope_list() -> None:
    for spec in iter_scope_specs():
        steps = ",".join(spec.supported_steps)
        print(f"{spec.key}\t{spec.namespace}\t{steps}\t{spec.label}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scopes",
        nargs="*",
        help="Scope refs such as polymarket:wc2026 or polymarket_wc2026.",
    )
    parser.add_argument(
        "--step",
        choices=SCOPE_STEPS,
        default="full",
        help="Fixed Dagster step to run for each scope.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_scopes",
        help="List known scopes and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Run remaining scopes after a failed job.",
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=Path(sys.executable),
        help="Python executable used for Dagster job execution.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list_scopes:
        _print_scope_list()
        return 0
    if not args.scopes:
        parser.error("at least one scope is required unless --list is used")

    status = 0
    for ref in args.scopes:
        try:
            spec = get_scope_spec(ref)
        except ValueError as exc:
            parser.error(str(exc))
        command = dagster_command(python=args.python, spec=spec, step=args.step)
        print(shlex.join(command))
        if args.dry_run:
            continue
        result = subprocess.run(command, cwd=REPO_ROOT, check=False)
        if result.returncode == 0:
            continue
        status = result.returncode
        if not args.continue_on_error:
            return status
    return status


if __name__ == "__main__":
    raise SystemExit(main())
