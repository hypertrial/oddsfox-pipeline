#!/usr/bin/env python3
"""Build an immutable consumer-neutral Polymarket graph catalog release."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

from oddsfox_pipeline.config import settings  # noqa: E402
from oddsfox_pipeline.publishing.polymarket_catalog import (  # noqa: E402
    DEFAULT_POLYMARKET_CATALOG_RELEASE_ROOT,
    build_polymarket_catalog_release,
)
from oddsfox_pipeline.storage.duckdb.connection import (  # noqa: E402
    open_duckdb_connection,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_POLYMARKET_CATALOG_RELEASE_ROOT,
    )
    parser.add_argument("--duckdb-path", type=Path, default=settings.DUCKDB_PATH)
    args = parser.parse_args(argv)
    conn = open_duckdb_connection(args.duckdb_path.resolve(), read_only=True)
    try:
        summary = build_polymarket_catalog_release(
            conn,
            args.output_root,
            dataset_version=args.dataset_version,
        )
    except (duckdb.Error, FileExistsError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()
    print(f"Published {summary['rows']:,} graph records under {summary['release_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
