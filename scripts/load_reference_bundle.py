#!/usr/bin/env python3
"""Validate and transactionally load a Scraper ``oddsfox.reference.v1`` bundle."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

from oddsfox_pipeline.contracts.reference_bundle import load_reference_bundle
from oddsfox_pipeline.contracts.reference_transport import (
    materialize_reference_bundle,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--warehouse", required=True, type=Path)
    parser.add_argument(
        "--cache-root", type=Path, default=Path(".cache/reference-bundles")
    )
    args = parser.parse_args(argv)
    approved_hosts = frozenset(
        host.strip().casefold()
        for host in os.getenv("ODDSFOX_REFERENCE_ARTIFACT_HOSTS", "").split(",")
        if host.strip()
    )
    bundle = materialize_reference_bundle(
        args.bundle,
        cache_root=args.cache_root,
        approved_hosts=approved_hosts,
    )
    manifest = load_reference_bundle(bundle, args.warehouse)
    print(json.dumps({"bundle_id": manifest["bundle_id"], "valid": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
