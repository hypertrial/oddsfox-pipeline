#!/usr/bin/env python3
"""Build an immutable WC2026 stage-market minute strategy input release."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT = ensure_src_on_path()

from oddsfox_pipeline.config import settings  # noqa: E402
from oddsfox_pipeline.publishing.stage_minute_inputs import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT,
    StageMinuteReleaseSpec,
    build_stage_minute_release,
    current_generator_commit,
)
from oddsfox_pipeline.storage.duckdb.connection import (  # noqa: E402
    open_duckdb_connection,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes-path", type=Path, required=True)
    parser.add_argument("--edges-path", type=Path, required=True)
    parser.add_argument("--graph-revision", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--duckdb-path", type=Path, default=settings.DUCKDB_PATH)
    args = parser.parse_args(argv)
    conn = open_duckdb_connection(args.duckdb_path.resolve(), read_only=True)
    try:
        summary = build_stage_minute_release(
            conn,
            args.output_root,
            StageMinuteReleaseSpec(
                dataset_version=args.dataset_version,
                graph_revision=args.graph_revision,
            ),
            nodes_path=args.nodes_path,
            edges_path=args.edges_path,
            generator_commit=current_generator_commit(REPO_ROOT),
        )
    except (
        duckdb.Error,
        FileExistsError,
        FileNotFoundError,
        LookupError,
        RuntimeError,
        ValueError,
    ) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()
    print(f"Built stage-minute release under {summary['release_dir']}")
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
