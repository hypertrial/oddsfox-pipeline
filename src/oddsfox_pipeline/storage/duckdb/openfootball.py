"""Persistence for the OpenFootball WC2026 knockout fixture mirror."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import duckdb

from oddsfox_pipeline.storage.duckdb.connection import _use_conn
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    openfootball_wc2026_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.openfootball import (
    bootstrap_openfootball_tables,
)

_COLUMNS = (
    "fifa_match_id",
    "stage_key",
    "stage_rank",
    "kickoff_at_utc",
    "home_team",
    "away_team",
    "venue",
    "match_status",
    "source_url",
    "source_line_number",
    "source_line_hash",
    "source_loaded_at",
)


def replace_knockout_fixtures(
    rows: Sequence[Mapping[str, object]],
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, int]:
    table = openfootball_wc2026_raw_tbl("knockout_fixtures")
    with _use_conn(conn) as active:
        bootstrap_openfootball_tables(active)
        active.execute("BEGIN TRANSACTION")
        try:
            deleted = active.execute(f"DELETE FROM {table}").fetchone()[0]
            if rows:
                placeholders = ", ".join(["?"] * len(_COLUMNS))
                active.executemany(
                    f"INSERT INTO {table} ({', '.join(_COLUMNS)}) "
                    f"VALUES ({placeholders})",
                    [tuple(row.get(column) for column in _COLUMNS) for row in rows],
                )
            active.execute("COMMIT")
        except Exception:
            active.execute("ROLLBACK")
            raise
    return {"deleted_rows": int(deleted), "inserted_rows": len(rows)}


__all__ = ["replace_knockout_fixtures"]
