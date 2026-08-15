#!/usr/bin/env python3
"""Export WC2026 national-team Elo freezes (pre-kickoff + latest current) as CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Final

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path
from _export_common import mart_exists as _mart_exists
from _export_common import qualified_mart_name

REPO_ROOT: Final[Path] = ensure_src_on_path()
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (  # noqa: E402
    WC2026_MARTS_SCHEMA,
)

MART_SCHEMA: Final = WC2026_MARTS_SCHEMA
CURRENT_MART: Final = "team_ratings_current"
HISTORY_MART: Final = "team_ratings_history"
PRE_KICKOFF_YEAR: Final = 2025
PRE_KICKOFF_SCOPE: Final = "2025"
PRE_KICKOFF_AS_OF: Final = "2025-12-31"
PRE_KICKOFF_FILE: Final = "team_ratings_pre_kickoff.csv"
LATEST_CURRENT_FILE: Final = "team_ratings_latest_current.csv"


def _require_mart(conn: duckdb.DuckDBPyConnection, mart_name: str) -> str:
    if not _mart_exists(conn, MART_SCHEMA, mart_name):
        raise LookupError(f"Missing {MART_SCHEMA}.{mart_name}. Run dbt build first.")
    return qualified_mart_name(MART_SCHEMA, mart_name)


def export_eloratings_wc2026_team_ratings_freezes(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
) -> dict[str, int]:
    """Write pre-kickoff (year-end 2025) and latest-current Elo CSVs.

    Returns a mapping of freeze label to exported row count.
    """
    history_rel = _require_mart(conn, HISTORY_MART)
    current_rel = _require_mart(conn, CURRENT_MART)
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    pre_path = output_dir / PRE_KICKOFF_FILE
    latest_path = output_dir / LATEST_CURRENT_FILE

    pre_count = conn.execute(
        f"""
        select count(*)
        from {history_rel}
        where snapshot_year = ?
          and snapshot_scope = ?
        """,
        [PRE_KICKOFF_YEAR, PRE_KICKOFF_SCOPE],
    ).fetchone()
    if not pre_count or int(pre_count[0]) == 0:
        raise LookupError(
            f"No {MART_SCHEMA}.{HISTORY_MART} rows for snapshot_year="
            f"{PRE_KICKOFF_YEAR} (pre-kickoff freeze)."
        )

    # DuckDB binds COPY TO ? before SELECT placeholders; keep the path literal.
    pre_path_sql = str(pre_path).replace("'", "''")
    conn.execute(
        f"""
        copy (
            select
                rank,
                team_code,
                team_name,
                rating,
                'pre_kickoff' as freeze_label,
                cast(? as varchar) as as_of,
                snapshot_id,
                collected_at
            from {history_rel}
            where snapshot_year = ?
              and snapshot_scope = ?
            order by rank, team_code
        ) to '{pre_path_sql}' (format csv, header true)
        """,
        [PRE_KICKOFF_AS_OF, PRE_KICKOFF_YEAR, PRE_KICKOFF_SCOPE],
    )

    latest_count_row = conn.execute(f"select count(*) from {current_rel}").fetchone()
    latest_count = int(latest_count_row[0]) if latest_count_row else 0
    if latest_count == 0:
        raise LookupError(
            f"No rows in {MART_SCHEMA}.{CURRENT_MART} (latest_current freeze)."
        )

    latest_path_sql = str(latest_path).replace("'", "''")
    conn.execute(
        f"""
        copy (
            select
                rank,
                team_code,
                team_name,
                rating,
                'latest_current' as freeze_label,
                cast(collected_at as varchar) as as_of,
                snapshot_id,
                collected_at
            from {current_rel}
            order by rank, team_code
        ) to '{latest_path_sql}' (format csv, header true)
        """
    )

    return {
        "pre_kickoff": int(pre_count[0]),
        "latest_current": latest_count,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", type=Path, default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "artifacts" / "wc2026_elo_exports",
    )
    parser.add_argument(
        "--read-only",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    args = parser.parse_args(argv)

    from oddsfox_pipeline.config import settings
    from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

    duck = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    conn = open_duckdb_connection(duck, read_only=args.read_only)
    try:
        counts = export_eloratings_wc2026_team_ratings_freezes(conn, args.output_dir)
    except LookupError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()

    out = args.output_dir.resolve()
    print(
        f"Exported pre_kickoff={counts['pre_kickoff']} rows to {out / PRE_KICKOFF_FILE}"
    )
    print(
        f"Exported latest_current={counts['latest_current']} rows to "
        f"{out / LATEST_CURRENT_FILE}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
