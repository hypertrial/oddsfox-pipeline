#!/usr/bin/env python3
"""Export Polymarket market catalog marts from the local DuckDB warehouse."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Sequence

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path
from _export_common import mart_exists as _mart_exists
from _export_common import qualified_mart_name, snapshot_duckdb_files

REPO_ROOT: Final[Path] = ensure_src_on_path()
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (  # noqa: E402
    POLYMARKET_WC2026_MARTS_SCHEMA,
)

CATALOG_MARTS: Final[tuple[tuple[str, str], ...]] = (
    (POLYMARKET_WC2026_MARTS_SCHEMA, "polymarket_wc2026_markets"),
)
SCOPE_MARTS: Final[dict[str, tuple[tuple[str, str], ...]]] = {
    "all": CATALOG_MARTS,
    "wc2026": (CATALOG_MARTS[0],),
}
DEFAULT_OUTPUT_DIR: Final[Path] = REPO_ROOT / "artifacts" / "polymarket_markets_exports"
MIN_VOLUME_USD: Final = 100_000.0
REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "event_id",
    "event_slug",
    "market_id",
    "question",
    "description",
    "outcomes",
    "clob_token_ids",
    "volume",
    "start_time",
    "end_time",
    "category",
    "tags",
)


def mart_exists(conn: duckdb.DuckDBPyConnection, schema: str, mart_name: str) -> bool:
    return _mart_exists(conn, schema, mart_name)


def validate_catalog_export(
    conn: duckdb.DuckDBPyConnection, parquet_path: Path
) -> dict[str, Any]:
    """Fail closed on grain, volume floor, timing, and outcomes/CLOB JSON quality."""
    path = str(parquet_path.resolve())
    cols = [
        row[0]
        for row in conn.execute(
            "describe select * from read_parquet(?)", [path]
        ).fetchall()
    ]
    missing_cols = [name for name in REQUIRED_COLUMNS if name not in cols]
    if missing_cols:
        raise ValueError(f"{parquet_path.name} missing columns: {missing_cols}")

    stats = conn.execute(
        """
        select
          count(*) as row_count,
          count(distinct market_id) as distinct_market_id,
          count(*) filter (where market_id is null or trim(market_id) = '') as bad_market_id,
          count(*) filter (where question is null or trim(question) = '') as bad_question,
          count(*) filter (where outcomes is null or trim(outcomes) = '') as bad_outcomes,
          count(*) filter (where volume is null or volume < ?) as below_floor,
          count(*) filter (
            where start_time is not null and end_time is not null and start_time > end_time
          ) as start_after_end
        from read_parquet(?)
        """,
        [MIN_VOLUME_USD, path],
    ).fetchone()
    assert stats is not None
    (
        row_count,
        distinct_market_id,
        bad_market_id,
        bad_question,
        bad_outcomes,
        below_floor,
        start_after_end,
    ) = stats
    if row_count <= 0:
        raise ValueError(f"{parquet_path.name} is empty")
    if distinct_market_id != row_count:
        raise ValueError(
            f"{parquet_path.name} duplicate market_id "
            f"(rows={row_count}, distinct={distinct_market_id})"
        )
    if bad_market_id or bad_question or bad_outcomes:
        raise ValueError(
            f"{parquet_path.name} null/empty required fields "
            f"(market_id={bad_market_id}, question={bad_question}, outcomes={bad_outcomes})"
        )
    if below_floor:
        raise ValueError(
            f"{parquet_path.name} has {below_floor} rows below ${MIN_VOLUME_USD:g} volume floor"
        )
    if start_after_end:
        raise ValueError(
            f"{parquet_path.name} has {start_after_end} rows with start_time > end_time"
        )

    bad_json = 0
    len_mismatch = 0
    for outcomes, clob_token_ids in conn.execute(
        "select outcomes, clob_token_ids from read_parquet(?)", [path]
    ).fetchall():
        try:
            outcome_list = json.loads(outcomes)
        except (TypeError, json.JSONDecodeError):
            bad_json += 1
            continue
        if not isinstance(outcome_list, list) or not outcome_list:
            bad_json += 1
            continue
        if clob_token_ids is None:
            continue
        try:
            token_list = json.loads(clob_token_ids)
        except (TypeError, json.JSONDecodeError):
            bad_json += 1
            continue
        if not isinstance(token_list, list):
            bad_json += 1
            continue
        if len(token_list) != len(outcome_list):
            len_mismatch += 1
    if bad_json:
        raise ValueError(
            f"{parquet_path.name} has {bad_json} invalid outcomes/CLOB JSON rows"
        )
    if len_mismatch:
        raise ValueError(
            f"{parquet_path.name} has {len_mismatch} outcomes/CLOB length mismatches"
        )

    return {
        "row_count": int(row_count),
        "distinct_market_id": int(distinct_market_id),
        "min_volume_usd": MIN_VOLUME_USD,
    }


def export_polymarket_markets_catalog(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    mart_name: str,
    output_path: Path,
    *,
    validate: bool = True,
) -> int:
    if not mart_exists(conn, schema, mart_name):
        raise LookupError(f"Missing {schema}.{mart_name}. Run dbt build first.")
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rel = qualified_mart_name(schema, mart_name)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    try:
        conn.execute(
            f"copy (select * from {rel}) to ? (format parquet)", [str(tmp_path)]
        )
        if validate:
            validate_catalog_export(conn, tmp_path)
        tmp_path.replace(output_path)
    except BaseException:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    row = conn.execute(f"select count(*) from {rel}").fetchone()
    return int(row[0]) if row else 0


def export_all_polymarket_markets_catalogs(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    *,
    timestamp: str | None = None,
    marts: Sequence[tuple[str, str]] = CATALOG_MARTS,
    validate: bool = True,
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
        count = export_polymarket_markets_catalog(
            conn, schema, mart_name, output_path, validate=validate
        )
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
        choices=("all", "wc2026"),
        default="all",
        help="Which catalog mart(s) to export (default: both).",
    )
    p.add_argument("--read-only", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--snapshot-copy", action="store_true")
    p.add_argument(
        "--validate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail closed on grain/volume/JSON checks (default: on).",
    )
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
            conn, Path(args.output_dir), marts=selected, validate=args.validate
        )
    except (LookupError, ValueError) as exc:
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
