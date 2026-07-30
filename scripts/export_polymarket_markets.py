#!/usr/bin/env python3
"""Export Polymarket market catalog marts from the local DuckDB warehouse."""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, Sequence

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path
from _export_common import mart_exists as _mart_exists
from _export_common import qualified_mart_name, snapshot_duckdb_files

REPO_ROOT: Final[Path] = ensure_src_on_path()
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (  # noqa: E402
    POLYMARKET_US_MIDTERMS_2026_MARTS_SCHEMA,
    POLYMARKET_WC2026_MARTS_SCHEMA,
)

CATALOG_MARTS: Final[tuple[tuple[str, str], ...]] = (
    (POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_markets"),
    (POLYMARKET_US_MIDTERMS_2026_MARTS_SCHEMA, "polymarket_us_midterms_2026_markets"),
)
SCOPE_MARTS: Final[dict[str, tuple[tuple[str, str], ...]]] = {
    "all": CATALOG_MARTS,
    "wc2026": (CATALOG_MARTS[0],),
    "us_midterms_2026": (CATALOG_MARTS[1],),
}
DEFAULT_OUTPUT_DIR: Final[Path] = REPO_ROOT / "artifacts" / "polymarket_markets_exports"


def mart_exists(conn: duckdb.DuckDBPyConnection, schema: str, mart_name: str) -> bool:
    return _mart_exists(conn, schema, mart_name)


def export_polymarket_markets_catalog(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    mart_name: str,
    output_path: Path,
) -> int:
    if not mart_exists(conn, schema, mart_name):
        raise LookupError(f"Missing {schema}.{mart_name}. Run dbt build first.")
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rel = qualified_mart_name(schema, mart_name)
    conn.execute(
        f"copy (select * from {rel}) to ? (format parquet)", [str(output_path)]
    )
    row = conn.execute(f"select count(*) from {rel}").fetchone()
    return int(row[0]) if row else 0


def export_all_polymarket_markets_catalogs(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    *,
    timestamp: str | None = None,
    marts: Sequence[tuple[str, str]] = CATALOG_MARTS,
) -> list[tuple[str, Path, int]]:
    missing = [
        f"{schema}.{mart_name}"
        for schema, mart_name in marts
        if not mart_exists(conn, schema, mart_name)
    ]
    if missing:
        raise LookupError(
            "Missing "
            + ", ".join(missing)
            + ". Run dbt build first (or pass --scope for available marts)."
        )
    ts = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results: list[tuple[str, Path, int]] = []
    for schema, mart_name in marts:
        output_path = output_dir / f"{mart_name}_{ts}.parquet"
        count = export_polymarket_markets_catalog(conn, schema, mart_name, output_path)
        results.append((f"{schema}.{mart_name}", output_path, count))
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duckdb-path", type=Path, default=None)
    p.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    p.add_argument(
        "--scope",
        choices=("all", "wc2026", "us_midterms_2026"),
        default="all",
        help="Which catalog mart(s) to export (default: both).",
    )
    p.add_argument("--read-only", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--snapshot-copy", action="store_true")
    args = p.parse_args(argv)

    from oddsfox_pipeline.config import settings
    from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

    duck = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    selected = SCOPE_MARTS[args.scope]

    profile_path = duck
    snap_dir: Path | None = None
    if args.snapshot_copy:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        snap_dir = Path(
            tempfile.mkdtemp(
                prefix="polymarket_markets_snap_", dir=str(args.output_dir)
            )
        )
        try:
            profile_path = snapshot_duckdb_files(duck, snap_dir)
        except BaseException:
            shutil.rmtree(snap_dir, ignore_errors=True)
            raise

    conn = open_duckdb_connection(profile_path, read_only=args.read_only)
    try:
        results = export_all_polymarket_markets_catalogs(
            conn, Path(args.output_dir), marts=selected
        )
    except LookupError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()
        if snap_dir is not None:
            shutil.rmtree(snap_dir, ignore_errors=True)

    for relation, output_path, row_count in results:
        print(f"Exported {row_count} rows from {relation} to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
