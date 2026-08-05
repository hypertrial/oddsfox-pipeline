#!/usr/bin/env python3
"""Export every present ``*_marts`` table from the local DuckDB warehouse to Parquet.

Discovers base tables in the shipped mart schemas (Polymarket WC2026, Kalshi
WC2026, international-results WC2026, and ``wc2026_marts``) and writes one
Parquet file per table under a timestamped output directory.

This is a local operator dump. It includes isolated pipeline marts when they
exist in the warehouse (match-minute, order book, market portrait, Polygon
settlement). For the allowlisted Polygon technical dossier, use
``export_polymarket_wc2026_polygon_settlement_minute_odds.py`` instead.
"""

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
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (  # noqa: E402
    INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA,
    KALSHI_WC2026_MARTS_SCHEMA,
    POLYMARKET_WC2026_MARTS_SCHEMA,
    WC2026_MARTS_SCHEMA,
)

DEFAULT_MART_SCHEMAS: Final[tuple[str, ...]] = (
    POLYMARKET_WC2026_MARTS_SCHEMA,
    KALSHI_WC2026_MARTS_SCHEMA,
    INTERNATIONAL_RESULTS_WC2026_MARTS_SCHEMA,
    WC2026_MARTS_SCHEMA,
)


def list_mart_tables(
    conn: duckdb.DuckDBPyConnection,
    schemas: tuple[str, ...] = DEFAULT_MART_SCHEMAS,
) -> list[tuple[str, str]]:
    if not schemas:
        return []
    placeholders = ", ".join("?" for _ in schemas)
    rows = conn.execute(
        f"""
        select table_schema, table_name
        from information_schema.tables
        where table_type = 'BASE TABLE'
          and table_schema in ({placeholders})
        order by table_schema, table_name
        """,
        list(schemas),
    ).fetchall()
    return [(str(schema), str(name)) for schema, name in rows]


def export_mart_table(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    mart_name: str,
    output_path: Path,
) -> int:
    if not _mart_exists(conn, schema, mart_name):
        raise LookupError(f"Missing {schema}.{mart_name}")
    rel = qualified_mart_name(schema, mart_name)
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(
        f"copy (select * from {rel}) to ? (format parquet)",
        [str(output_path)],
    )
    row = conn.execute(f"select count(*) from {rel}").fetchone()
    return int(row[0]) if row else 0


def export_all_marts(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    *,
    schemas: tuple[str, ...] = DEFAULT_MART_SCHEMAS,
) -> list[tuple[str, str, Path, int]]:
    tables = list_mart_tables(conn, schemas)
    if not tables:
        raise LookupError(
            "No mart tables found in schemas "
            + ", ".join(schemas)
            + ". Run dbt build first."
        )
    results: list[tuple[str, str, Path, int]] = []
    for schema, mart_name in tables:
        out = output_dir / f"{schema}.{mart_name}.parquet"
        rows = export_mart_table(conn, schema, mart_name, out)
        results.append((schema, mart_name, out, rows))
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duckdb-path", type=Path, default=None)
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for Parquet files (default: artifacts/marts_exports/<utc>).",
    )
    p.add_argument(
        "--schema",
        action="append",
        dest="schemas",
        default=None,
        help="Limit to one mart schema (repeatable). Default: all shipped mart schemas.",
    )
    p.add_argument("--read-only", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--snapshot-copy", action="store_true")
    args = p.parse_args(argv)

    from oddsfox_pipeline.config import settings
    from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

    duck = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    schemas = tuple(args.schemas) if args.schemas else DEFAULT_MART_SCHEMAS
    unknown = [s for s in schemas if s not in DEFAULT_MART_SCHEMAS]
    if unknown:
        sys.stderr.write(
            "Unknown mart schema(s): "
            + ", ".join(unknown)
            + f". Expected one of: {', '.join(DEFAULT_MART_SCHEMAS)}\n"
        )
        return 2

    if args.output_dir is not None:
        output_dir = Path(args.output_dir).resolve()
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = (REPO_ROOT / "artifacts" / "marts_exports" / ts).resolve()

    profile_path = duck
    snap_dir: Path | None = None
    if args.snapshot_copy:
        output_dir.mkdir(parents=True, exist_ok=True)
        snap_dir = Path(
            tempfile.mkdtemp(prefix="marts_export_snap_", dir=str(output_dir))
        )
        try:
            profile_path = snapshot_duckdb_files(duck, snap_dir)
        except BaseException:
            shutil.rmtree(snap_dir, ignore_errors=True)
            raise

    conn = open_duckdb_connection(profile_path, read_only=args.read_only)
    try:
        results = export_all_marts(conn, output_dir, schemas=schemas)
    except LookupError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()
        if snap_dir is not None:
            shutil.rmtree(snap_dir, ignore_errors=True)

    total_rows = 0
    for schema, mart_name, path, rows in results:
        total_rows += rows
        print(f"{schema}.{mart_name}: {rows} rows -> {path}")
    print(f"Exported {len(results)} marts ({total_rows} total rows) to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
