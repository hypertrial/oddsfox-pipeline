#!/usr/bin/env python3
"""Build a sanitized technical export from a Polygon settlement audit release."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT = ensure_src_on_path()

from oddsfox_pipeline.publishing.polygon_settlement_export import (  # noqa: E402
    DEFAULT_POLYGON_SETTLEMENT_EXPORT_ROOT,
    export_polygon_settlement_minute_odds,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-release", type=Path, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_POLYGON_SETTLEMENT_EXPORT_ROOT,
    )
    args = parser.parse_args(argv)

    try:
        summary = export_polygon_settlement_minute_odds(
            args.audit_release,
            args.output_root,
            repo_root=REPO_ROOT,
        )
    except (FileExistsError, OSError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1

    print(
        f"Exported {summary['rows']:,} rows under {summary['release_dir']} "
        f"(SHA-256 {summary['csv_sha256']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
