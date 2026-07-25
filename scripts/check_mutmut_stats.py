#!/usr/bin/env python3
"""Fail unless a Mutmut run has no unresolved mutants."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

COUNTERS = {
    "killed",
    "survived",
    "total",
    "no_tests",
    "skipped",
    "suspicious",
    "timeout",
    "check_was_interrupted_by_user",
    "segfault",
}
UNRESOLVED = COUNTERS - {"killed", "total", "skipped"}


def validate_stats(stats: Any) -> None:
    if not isinstance(stats, dict):
        raise ValueError("report must be a JSON object")

    missing = COUNTERS - stats.keys()
    if missing:
        raise ValueError(f"missing counters: {', '.join(sorted(missing))}")

    invalid = {
        name
        for name in COUNTERS
        if isinstance(stats[name], bool)
        or not isinstance(stats[name], int)
        or stats[name] < 0
    }
    if invalid:
        raise ValueError(
            f"counters must be non-negative integers: {', '.join(sorted(invalid))}"
        )

    if stats["total"] == 0:
        raise ValueError("no mutants were generated")

    unresolved = {name: stats[name] for name in UNRESOLVED if stats[name]}
    if unresolved:
        details = ", ".join(
            f"{name}={value}" for name, value in sorted(unresolved.items())
        )
        raise ValueError(f"unresolved mutants: {details}")

    if stats["killed"] + stats["skipped"] != stats["total"]:
        raise ValueError("killed + skipped must equal total")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "report",
        nargs="?",
        type=Path,
        default=Path("mutants/mutmut-cicd-stats.json"),
    )
    args = parser.parse_args(argv)

    try:
        stats = json.loads(args.report.read_text(encoding="utf-8"))
        validate_stats(stats)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Mutmut statistics failed: {exc}", file=sys.stderr)
        return 1

    print(
        "Mutmut statistics passed "
        f"({stats['killed']} killed, {stats['skipped']} skipped)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
