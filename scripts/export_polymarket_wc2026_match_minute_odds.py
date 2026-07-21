#!/usr/bin/env python3
"""Export and summarize the WC2026 match-minute odds mart as Parquet."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Final

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path
from _export_common import mart_exists as _mart_exists
from _export_common import qualified_mart_name

REPO_ROOT: Final[Path] = ensure_src_on_path()
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (  # noqa: E402
    POLYMARKET_WC2026_MARTS_SCHEMA,
)

MART_SCHEMA: Final = POLYMARKET_WC2026_MARTS_SCHEMA
MART_NAME: Final = "polymarket_wc2026_match_minute_odds"
DEFAULT_OUTPUT: Final = (
    REPO_ROOT / "artifacts" / "polymarket_wc2026_exports" / f"{MART_NAME}.parquet"
)


def mart_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    return _mart_exists(conn, MART_SCHEMA, MART_NAME)


def export_polymarket_wc2026_match_minute_odds(
    conn: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> None:
    if not mart_exists(conn):
        raise LookupError(f"Missing {MART_SCHEMA}.{MART_NAME}. Run dbt build first.")
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    relation = qualified_mart_name(MART_SCHEMA, MART_NAME)
    conn.execute(
        f"copy (select * from {relation}) to ? (format parquet)",
        [str(output_path)],
    )


def summarize_parquet(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> dict[str, Any]:
    row = conn.execute(
        """
        select
            count(*) as rows,
            count(distinct fifa_match_id) as fifa_matches,
            count(distinct market_id) as markets,
            min(odds_minute_utc) as first_minute_utc,
            max(odds_minute_utc) as last_minute_utc,
            count(*) filter (where yes_observed) as yes_observed_minutes,
            count(*) filter (where no_observed) as no_observed_minutes,
            count(*) filter (where minute_complete) as complete_minutes
        from read_parquet(?)
        """,
        [str(parquet_path.resolve())],
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Could not summarize {parquet_path}")
    rows = int(row[0])
    return {
        "rows": rows,
        "fifa_matches": int(row[1]),
        "markets": int(row[2]),
        "first_minute_utc": row[3],
        "last_minute_utc": row[4],
        "yes_observed_minutes": int(row[5]),
        "no_observed_minutes": int(row[6]),
        "complete_minutes": int(row[7]),
        "minute_completeness_pct": round(100 * int(row[7]) / rows, 2)
        if rows
        else 0.0,
        "file_bytes": parquet_path.stat().st_size,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--read-only", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args(argv)

    from oddsfox_pipeline.config import settings
    from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

    duckdb_path = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    output_path = args.output.resolve()
    conn = open_duckdb_connection(duckdb_path, read_only=args.read_only)
    try:
        export_polymarket_wc2026_match_minute_odds(conn, output_path)
        summary = summarize_parquet(conn, output_path)
    except LookupError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()

    print(f"Exported {summary['rows']:,} rows to {output_path}")
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
