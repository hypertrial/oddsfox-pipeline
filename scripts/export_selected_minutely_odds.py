#!/usr/bin/env python3
"""
Export the full selected-scope minutely odds mart to a parquet file.

Reads ``polymarket_marts.selected_token_minutely_odds`` from the local DuckDB
warehouse and writes a single parquet file via DuckDB ``COPY``. Also writes a
companion markdown data spec alongside the parquet (``.md`` with the same stem).

By default opens DuckDB **read-only**. Use ``--snapshot-copy`` when Dagster or
another job already holds a write connection on the live warehouse file.

Usage:
  python3 scripts/export_selected_minutely_odds.py
  python3 scripts/export_selected_minutely_odds.py --snapshot-copy
  python3 scripts/export_selected_minutely_odds.py --output /tmp/selected_minutely.parquet
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT: Final[Path] = ensure_src_on_path()

MART_SCHEMA: Final = "polymarket_marts"
MART_NAME: Final = "selected_token_minutely_odds"

COLUMN_DOCS: Final[dict[str, str]] = {
    "market_id": "Polymarket market identifier.",
    "outcome_index": "Zero-based outcome index within the market token list.",
    "clob_token_id": "CLOB outcome token identifier (grain key).",
    "question": "Market question or title at export time.",
    "outcome_label": (
        "Resolved outcome label (e.g. Yes/No) from the market outcomes array "
        "at outcome_index."
    ),
    "event_slug": "Polymarket event slug for the selected market scope.",
    "is_active": "Whether the market is active at export time.",
    "is_closed": "Whether the market is closed at export time.",
    "market_volume_usd": "Reported market volume (USD) at build time.",
    "odds_timestamp": "Wall-clock timestamp of the minutely odds observation.",
    "ODDS_TIMESTAMP": "Wall-clock timestamp of the minutely odds observation.",
    "odds_timestamp_epoch": "Unix epoch seconds for the observation (grain key).",
    "ODDS_TIMESTAMP_EPOCH": "Unix epoch seconds for the observation (grain key).",
    "price": "Outcome implied probability in [0, 1] (Polymarket CLOB price).",
}


@dataclass(frozen=True)
class ExportStats:
    row_count: int
    market_count: int
    token_count: int
    min_epoch: int | None
    max_epoch: int | None
    min_price: float | None
    max_price: float | None
    null_outcome_labels: int
    outcome_labels: list[tuple[str, int]]


def _snapshot_duckdb_files(src: Path, dest_dir: Path) -> Path:
    """Copy ``src`` and same-directory siblings (e.g. ``.wal``) for offline export."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(f for f in src.parent.glob(src.name + "*") if f.is_file())
    if not files:
        raise FileNotFoundError(
            f"No DuckDB files matched {src.name!r}* under {src.parent}"
        )
    for f in files:
        shutil.copy2(f, dest_dir / f.name)
    main = dest_dir / src.name
    if not main.is_file():
        raise FileNotFoundError(f"Expected {main} after snapshot copy")
    return main


def _mart_qualified_name() -> str:
    from oddsfox.storage.duckdb.profile.discovery import qualified_name

    return qualified_name(MART_SCHEMA, MART_NAME)


def mart_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    row = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ?
          and table_name = ?
        """,
        [MART_SCHEMA, MART_NAME],
    ).fetchone()
    return bool(row and row[0])


def fetch_export_stats(conn: duckdb.DuckDBPyConnection) -> ExportStats:
    rel = _mart_qualified_name()
    summary = conn.execute(
        f"""
        select
            count(*) as row_count,
            count(distinct market_id) as market_count,
            count(distinct clob_token_id) as token_count,
            min(odds_timestamp_epoch) as min_epoch,
            max(odds_timestamp_epoch) as max_epoch,
            min(price) as min_price,
            max(price) as max_price,
            count(*) filter (where outcome_label is null) as null_outcome_labels
        from {rel}
        """
    ).fetchone()
    if not summary:
        raise LookupError(f"Missing {MART_SCHEMA}.{MART_NAME}")

    labels = conn.execute(
        f"""
        select outcome_label, count(*) as rows
        from {rel}
        group by 1
        order by rows desc
        limit 10
        """
    ).fetchall()

    return ExportStats(
        row_count=int(summary[0]),
        market_count=int(summary[1]),
        token_count=int(summary[2]),
        min_epoch=int(summary[3]) if summary[3] is not None else None,
        max_epoch=int(summary[4]) if summary[4] is not None else None,
        min_price=float(summary[5]) if summary[5] is not None else None,
        max_price=float(summary[6]) if summary[6] is not None else None,
        null_outcome_labels=int(summary[7]),
        outcome_labels=[(str(label), int(rows)) for label, rows in labels],
    )


def _format_epoch(epoch: int | None) -> str:
    if epoch is None:
        return "n/a"
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _parquet_schema(
    conn: duckdb.DuckDBPyConnection, parquet_path: Path
) -> list[tuple[str, str]]:
    rows = conn.execute(
        "describe select * from read_parquet(?)",
        [str(parquet_path)],
    ).fetchall()
    return [(str(name), str(dtype)) for name, dtype, *_rest in rows]


def render_export_spec(
    *,
    parquet_path: Path,
    stats: ExportStats,
    exported_at: datetime,
    schema: list[tuple[str, str]],
) -> str:
    lines = [
        f"# {MART_NAME}",
        "",
        "## Overview",
        "",
        f"- **Source mart:** `{MART_SCHEMA}.{MART_NAME}`",
        "- **Grain:** one row per `(clob_token_id, odds_timestamp_epoch)`",
        f"- **Exported at (UTC):** {exported_at.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"- **Parquet file:** `{parquet_path.name}`",
        "",
        "## Snapshot",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Rows | {stats.row_count:,} |",
        f"| Markets | {stats.market_count:,} |",
        f"| Tokens | {stats.token_count:,} |",
        f"| Time range start (UTC) | {_format_epoch(stats.min_epoch)} |",
        f"| Time range end (UTC) | {_format_epoch(stats.max_epoch)} |",
        f"| Price min | {stats.min_price} |",
        f"| Price max | {stats.max_price} |",
        f"| Null outcome_label rows | {stats.null_outcome_labels:,} |",
        "",
        "## Schema",
        "",
        "| Column | Type | Description |",
        "| --- | --- | --- |",
    ]
    for name, dtype in schema:
        doc = COLUMN_DOCS.get(name, "")
        lines.append(f"| `{name}` | `{dtype}` | {doc} |")

    lines.extend(
        [
            "",
            "## Outcome labels (top 10)",
            "",
            "| outcome_label | rows |",
            "| --- | --- |",
        ]
    )
    for label, rows in stats.outcome_labels:
        display = label if label else "(null)"
        lines.append(f"| {display} | {rows:,} |")

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `outcome_label` resolves Yes/No (or named outcomes) without joining `selected_markets`.",
            "- `market_volume_usd`, `question`, and market state fields reflect build-time metadata.",
            "- DuckDB may uppercase exported timestamp columns (`ODDS_TIMESTAMP`, `ODDS_TIMESTAMP_EPOCH`).",
            "",
        ]
    )
    return "\n".join(lines)


def write_export_spec(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    stats: ExportStats,
    *,
    exported_at: datetime | None = None,
) -> Path:
    spec_path = parquet_path.with_suffix(".md")
    exported_at = exported_at or datetime.now(timezone.utc)
    schema = _parquet_schema(conn, parquet_path)
    spec_path.write_text(
        render_export_spec(
            parquet_path=parquet_path,
            stats=stats,
            exported_at=exported_at,
            schema=schema,
        ),
        encoding="utf-8",
    )
    return spec_path


def export_minutely_odds_parquet(
    conn: duckdb.DuckDBPyConnection,
    output_path: Path,
    *,
    write_spec: bool = True,
) -> tuple[int, Path | None]:
    """Export the selected-scope minutely odds mart; return row count and optional spec path."""
    if not mart_exists(conn):
        raise LookupError(
            f"Missing {MART_SCHEMA}.{MART_NAME}. Run dbt build or the pipeline first."
        )

    stats = fetch_export_stats(conn)
    rel = _mart_qualified_name()

    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(
        f"""
        copy (select * from {rel})
        to ? (format parquet)
        """,
        [str(output_path)],
    )

    spec_path: Path | None = None
    if write_spec:
        spec_path = write_export_spec(conn, output_path, stats)
    return stats.row_count, spec_path


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="DuckDB file (default: DUCKDB_PATH from settings / .env)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Destination parquet file "
            "(default: artifacts/selected_scope_exports/selected_token_minutely_odds_<UTC>.parquet)"
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "artifacts" / "selected_scope_exports",
        help="Directory for default timestamped output when --output is omitted",
    )
    p.add_argument(
        "--read-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Open DuckDB read-only (default: true). Use --no-read-only for read-write.",
    )
    p.add_argument(
        "--snapshot-copy",
        action="store_true",
        help=(
            "Copy the DuckDB file(s) into a temp folder under --output-dir, then "
            "export from the copy. Use when a writer already has the live file open."
        ),
    )
    p.add_argument(
        "--no-spec",
        action="store_true",
        help="Skip writing the companion markdown data spec.",
    )
    args = p.parse_args()

    from oddsfox.config import settings
    from oddsfox.storage.duckdb import open_duckdb_connection

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
            tempfile.mkdtemp(
                prefix="selected_minutely_export_snap_",
                dir=str(args.output_dir),
            )
        )
        try:
            profile_path = _snapshot_duckdb_files(duck, snap_dir)
        except BaseException:
            shutil.rmtree(snap_dir, ignore_errors=True)
            raise

    conn = open_duckdb_connection(profile_path, read_only=args.read_only)
    try:
        row_count, spec_path = export_minutely_odds_parquet(
            conn,
            output_path,
            write_spec=not args.no_spec,
        )
    except LookupError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()
        if snap_dir is not None:
            shutil.rmtree(snap_dir, ignore_errors=True)

    print(f"Exported {row_count} rows to {output_path}")
    if spec_path is not None:
        print(f"Wrote data spec to {spec_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
