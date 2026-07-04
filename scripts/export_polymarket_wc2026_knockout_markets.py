#!/usr/bin/env python3
"""Export graph-ready ``polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds``."""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path
from _export_common import mart_exists as _mart_exists
from _export_common import qualified_mart_name, snapshot_duckdb_files

REPO_ROOT: Final[Path] = ensure_src_on_path()
MART_SCHEMA: Final = "polymarket_wc2026_marts"
MART_NAME: Final = "polymarket_wc2026_knockout_token_hourly_odds"


def _qualified_name() -> str:
    return qualified_mart_name(MART_SCHEMA, MART_NAME)


def mart_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    return _mart_exists(conn, MART_SCHEMA, MART_NAME)


def export_polymarket_wc2026_knockout_markets(
    conn: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> int:
    if not mart_exists(conn):
        raise LookupError(f"Missing {MART_SCHEMA}.{MART_NAME}. Run dbt build first.")
    rel = _qualified_name()
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(
        f"copy (select * from {rel}) to ? (format parquet)", [str(output_path)]
    )
    row = conn.execute(f"select count(*) from {rel}").fetchone()
    return int(row[0]) if row else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duckdb-path", type=Path, default=None)
    p.add_argument("--output", type=Path, default=None)
    p.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "artifacts" / "polymarket_wc2026_exports",
    )
    p.add_argument("--read-only", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--snapshot-copy", action="store_true")
    args = p.parse_args(argv)

    from oddsfox_pipeline.config import settings
    from oddsfox_pipeline.storage.duckdb import open_duckdb_connection

    duck = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    if args.output is not None:
        output_path = Path(args.output).resolve()
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = args.output_dir / f"{MART_NAME}_{ts}.parquet"

    profile_path = duck
    snap_dir: Path | None = None
    if args.snapshot_copy:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        snap_dir = Path(
            tempfile.mkdtemp(prefix="polymarket_wc2026_knockout_snap_", dir=str(args.output_dir))
        )
        try:
            profile_path = snapshot_duckdb_files(duck, snap_dir)
        except BaseException:
            shutil.rmtree(snap_dir, ignore_errors=True)
            raise

    conn = open_duckdb_connection(profile_path, read_only=args.read_only)
    try:
        row_count = export_polymarket_wc2026_knockout_markets(conn, output_path)
    except LookupError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()
        if snap_dir is not None:
            shutil.rmtree(snap_dir, ignore_errors=True)

    print(f"Exported {row_count} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
