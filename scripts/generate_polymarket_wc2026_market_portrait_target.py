#!/usr/bin/env python3
"""Generate a non-credit-consuming PMXT target candidate for operator review."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from oddsfox_pipeline.config.settings import DUCKDB_PATH
from oddsfox_pipeline.publishing.market_portrait_target import (
    generate_target_manifest,
    write_target_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fifa-match-id", type=int, required=True)
    parser.add_argument("--warehouse", type=Path, default=DUCKDB_PATH)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or Path(".cache/market_portrait_targets") / (
        f"match-{args.fifa_match_id}.yml"
    )
    with duckdb.connect(str(args.warehouse), read_only=True) as connection:
        payload = generate_target_manifest(connection, fifa_match_id=args.fifa_match_id)
    print(write_target_manifest(payload, output).resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
