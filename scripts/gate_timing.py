#!/usr/bin/env python3
"""Opt-in gate timing harness. Writes ignored JSON under .cache/runtime/benchmarks/."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT = ensure_src_on_path()

DEFAULT_TARGETS = (
    "unit-orchestration",
    "test",
    "integration-dbt",
    "ci-fast",
)


def _benchmarks_dir() -> Path:
    root = Path(
        os.environ.get(
            "ODDSFOX_RUNTIME_ROOT",
            str(REPO_ROOT / ".cache" / "runtime"),
        )
    )
    path = root / "benchmarks"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _run_once(target: str) -> dict[str, object]:
    started = time.perf_counter()
    proc = subprocess.run(
        ["make", target],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
    )
    elapsed = time.perf_counter() - started
    return {
        "target": target,
        "elapsed_seconds": round(elapsed, 3),
        "returncode": proc.returncode,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "targets",
        nargs="*",
        default=list(DEFAULT_TARGETS),
        help="Make targets to time (default: unit-orchestration test integration-dbt ci-fast)",
    )
    parser.add_argument(
        "--label",
        default="local",
        help="Label stored in the result JSON (e.g. cold, warm, before, after)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of times to run each target",
    )
    args = parser.parse_args(argv)
    if args.repeat < 1:
        raise SystemExit("--repeat must be >= 1")

    results = {
        "label": args.label,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "cwd": str(REPO_ROOT),
        "runs": [],
    }
    for target in args.targets:
        for index in range(args.repeat):
            print(f"[gate-timing] {args.label} {target} ({index + 1}/{args.repeat})")
            run = _run_once(target)
            run["repeat_index"] = index
            results["runs"].append(run)
            if run["returncode"] != 0:
                break

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = _benchmarks_dir() / f"gate-timing-{args.label}-{stamp}.json"
    out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"[gate-timing] wrote {out}")
    return 0 if all(run["returncode"] == 0 for run in results["runs"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
