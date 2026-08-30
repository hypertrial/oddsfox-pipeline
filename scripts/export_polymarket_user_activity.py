#!/usr/bin/env python3
"""Export bounded public Polymarket user activity to daily Parquet files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

from oddsfox_pipeline.ingestion.polymarket.user_activity import (  # noqa: E402
    export_user_activity,
    parse_utc,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--replace",
        action="store_true",
        help="replace requested partitions when compatible output cannot be resumed",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    manifest = export_user_activity(
        wallet=args.wallet,
        start_utc=parse_utc(args.start_utc),
        end_utc=parse_utc(args.end_utc),
        output_dir=args.output_dir,
        replace=args.replace,
    )
    print(json.dumps({"rows": manifest["rows"], "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
