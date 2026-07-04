"""Persistence helpers for international-results match data."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import duckdb

from oddsfox_pipeline.storage.duckdb.connection import _use_conn
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    international_results_wc2026_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.international_results import (
    bootstrap_international_results_tables,
)

_MATCH_RESULT_COLUMNS = (
    "match_id",
    "match_date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "tournament",
    "city",
    "country",
    "neutral",
    "match_status",
    "source_url",
    "source_row_number",
    "source_row_hash",
    "source_loaded_at",
)


def replace_wc2026_match_results(
    rows: Sequence[Mapping[str, object]],
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, int]:
    """Replace the WC2026 raw match-results slice."""
    table = international_results_wc2026_raw_tbl("match_results")
    with _use_conn(conn) as active:
        bootstrap_international_results_tables(active)
        active.execute("BEGIN TRANSACTION")
        try:
            deleted = active.execute(f"DELETE FROM {table}").fetchone()[0]
            if rows:
                placeholders = ", ".join(["?"] * len(_MATCH_RESULT_COLUMNS))
                active.executemany(
                    f"""
                    INSERT INTO {table} ({", ".join(_MATCH_RESULT_COLUMNS)})
                    VALUES ({placeholders})
                    """,
                    [
                        tuple(row.get(column) for column in _MATCH_RESULT_COLUMNS)
                        for row in rows
                    ],
                )
            active.execute("COMMIT")
        except Exception:
            active.execute("ROLLBACK")
            raise
    return {"deleted_rows": int(deleted), "inserted_rows": len(rows)}


__all__ = ["replace_wc2026_match_results"]
